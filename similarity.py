
import hashlib
import re
from stop_words import get_stop_words

_STOP_WORDS = set(get_stop_words('en'))


class Similarity:

    def __init__(self, hash_bits=64):
        self.hash_bits = hash_bits
        self.threshold = 0.85
        self.url_exact_hashes = {}
        self.url_simhashes = {}

    def specific_hash (self, word):
        # DONT USE MD5
        # use SHA256
        return hashlib.md5(word.encode()).hexdigest()
    

    def extract_words(self, text):
        if not text:
            return {}

        # Extract alphanumeric sequences (need + to match multi-char words)
        words = re.findall(r'\b[a-z0-9]+\b', text.lower())

        word_freqs = {}
        for word in words:
            # Drop stop words and very short tokens — they're shared by every
            # page on the site and dominate the simhash with template noise.
            if word in _STOP_WORDS or len(word) < 3:
                continue
            word_freqs[word] = word_freqs.get(word, 0) + 1

        return word_freqs
    
    def pseudo_random_hash(self, word):
        
        # word.encode -> string into bytes
        # hashlib.md5 -> 128 bit hash object
        # hexdigest -> takes the 128 bit hash object, turns it into hex
        # int(..., 16) -> converts the hex string into an integer
        # & ((1 << 64) - 1) -> bitmasks last 64 bits
        # h = 64 bit pseudo random number
        h = self.specific_hash(word)
        hash_int = int(h, 16)
        return hash_int & ((1 << self.hash_bits) - 1)

    def print_sim_percentage(self, url1, url2):
        sim_percentage = self.hamming_distance(self.url_simhashes[url1], self.url_simhashes[url2])
        print(f"Similarity between {url1} and {url2}: {sim_percentage:.2%}")

    def simhash(self, words):
        '''
        hashes each word
        accumulates a weighted bit vector across all 64 bit positions
        produces a single 64-bit integer fingerprint
        '''
        v = [0] * 64 # list of 0's
        for word in words:

            h = self.pseudo_random_hash(word)

            for i in range(64):
                # if ith bit of h is on, add 1 to v[i], else subtract 1 from v[i]
                v[i] += 1 if h & (1 << i) else -1
        
        # all bits turned off
        fingerprint = 0
        for i in range(64):
            if v[i] > 0:
                # bitwise or 
                # (1 << i) is a number with only the ith bit on
                # basically turns on only that bit for the fingerprint
                fingerprint |= (1 << i) 

        return fingerprint

    def hamming_distance(self, h1, h2):
        ''' 
        counts how many bits differ between two fingerprints
        pages with very similar text will differ by only a few bits
        '''
        # h1 ^ h2 -> bitwise XOR, gives a number with bits on where h1 and h2 differ
        # bin(...) -> converts that number to a binary string, e.g. '0b101010'
        # .count('1') -> counts how many '1's are in that binary string
        # however many 1's show how different they actually are
        xor = h1 ^ h2
            
        hamming_distance = bin(xor).count('1')
        
        matching_bits = self.hash_bits - hamming_distance
        similarity_percentage = matching_bits / self.hash_bits
        return similarity_percentage

    def is_similar(self, url, text) : 
        '''
        Computes hashes from the page text and checks against previously
        stored hashes. Returns True if exact or near-duplicate found,
        False otherwise (and stores the hashes under `url` so future calls
        can compare against this page).

        returns tuple (bool / type)
        bool -> if its a duplicate or not
        type -> "exact" or "near" if its a duplicate, None otherwise
        '''
        if not text:
            return False, "new"

        # Exact-match hash on full page text
        exact_hash = self.specific_hash(text)

        # Build a frequency-weighted word list, then simhash.
        word_freqs = self.extract_words(text)
        if not word_freqs:
            return False, "new"
        words = []
        for word, freq in word_freqs.items():
            words.extend([word] * freq)

        fingerprint = self.simhash(words)

        '''
        # shingle weighted if we need it
        shingle_freqs = self.extract_shingles(text, n=3)
        if not shingle_freqs:
            return False
        tokens = []
        for shingle, freq in shingle_freqs.items():
            tokens.extend([shingle] * freq)

        fingerprint = self.simhash(tokens)
        '''

        # Exact-match check against any previously seen page
        for stored_hash in self.url_exact_hashes.values():
            if stored_hash == exact_hash:
                return True, "exact"

        # Near-duplicate check (hamming_distance returns similarity 0-1)
        for stored_simhash in self.url_simhashes.values():
            if self.hamming_distance(stored_simhash, fingerprint) >= self.threshold:
                return True, "near"

        # New page — store under this URL so future pages can compare
        self.url_exact_hashes[url] = exact_hash
        self.url_simhashes[url] = fingerprint
        return False, "new"

    # IF ITS TOO HARSH AND I DONT WANNA LOWER THE AMT
    def extract_shingles(self, text, n=3):
        """
        Generate n-word shingles from text. Each shingle is a single string
        like "department computer science" — treated as one 'word' by simhash.

        n=3 (trigrams) is the sweet spot: long enough to be distinctive,
        short enough that small edits don't change too many shingles.
        """
        if not text:
            return {}

        tokens = re.findall(r'\b[a-z0-9]+\b', text.lower())

        if len(tokens) < n:
            # Page too short to form even one shingle
            return {}

        shingle_freqs = {}
        for i in range(len(tokens) - n + 1):
            shingle = " ".join(tokens[i:i + n])
            shingle_freqs[shingle] = shingle_freqs.get(shingle, 0) + 1

        return shingle_freqs
