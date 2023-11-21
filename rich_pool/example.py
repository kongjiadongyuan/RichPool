import random
import time

from . import Pool, RichPoolExitResult


def test_func(status, idx):
    status["stage"] = "[green]Started"
    if idx == 0:
        status["stage"] = "test" * 0x200
        for _ in range(1000):
            time.sleep(0.5)
    else:
        t = random.randint(3, 6)
        time.sleep(t)
    status["stage"] = "[red]Sleeped"
    t = random.randint(3, 6)
    time.sleep(t)
    val = random.randint(0, 100)
    if val >= 50:
        raise Exception("???")
    status["stage"] = "[blue]Done"
    return idx**2


if __name__ == "__main__":
    pool = Pool(num_processes=64, pass_status=True)

    for i in range(128):
        pool.apply_async(test_func, args=(i,))

    while True:
        ret = pool.monitor()
        if ret == RichPoolExitResult.INTERACTIVE:
            pass
        elif ret == RichPoolExitResult.QUEUE_EMPTY:
            break
        elif ret == RichPoolExitResult.QUIT:
            choice = input("Quit? (yes/no): ")
            if "yes" in choice:
                break
        else:
            raise ValueError(ret)
        

    print(pool.results())
