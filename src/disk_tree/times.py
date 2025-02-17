from time import perf_counter

class Times:
    def __init__(self):
        self.times = {}
        self.cur_timer = None
        self.cur_start = 0

    def __call__(self, name: str | None = None) -> None:
        now = perf_counter()
        if self.cur_timer:
            self.times[self.cur_timer] = now - self.cur_start
        if name:
            self.cur_timer = name
            self.cur_start = perf_counter()
        else:
            self.cur_timer = None
            self.cur_start = 0

