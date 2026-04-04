import time
import random

def exponential_backoff(func, *args, **kwargs):
    # Jika yang masuk bukan fungsi (tapi angka/base), kita abaikan saja backoff-nya
    # agar tidak menyebabkan TypeError
    if not callable(func):
        return 0 
        
    max_retries = 5
    for i in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e):
                if i == max_retries - 1:
                    raise e
                wait_time = (20 * (2 ** i)) + random.random()
                print(f"⚠️ Limit habis. Menunggu {wait_time:.2f} detik sebelum coba lagi...")
                time.sleep(wait_time)
            else:
                raise e

class RateLimiter:
    def __init__(self, requests_per_minute=1): 
        # Kita set super lambat: 1 berita per menit. Lambat asal selamat.
        self.delay = 65 
        self.last_request_time = 0
        
    def wait(self):
        time.sleep(1)

    def wait_if_needed(self):
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < self.delay:
            remaining = self.delay - elapsed
            print(f"☕ Menunggu jeda antar berita ({remaining:.0f} detik)...")
            time.sleep(remaining)
        self.last_request_time = time.time()

    def daily_quota_exhausted(self):
        return False