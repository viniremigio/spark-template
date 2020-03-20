import sys
from process_local import run_job

if __name__ == '__main__':

    input_data = sys.argv[1]
    run_job(input_data)

    print("Done!")
