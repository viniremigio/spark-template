import sys
from process import run_job

if __name__ == '__main__':

    input = sys.argv[1]
    run_job(input)

    print("Done!")
