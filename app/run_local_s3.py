import sys
from process_local_s3 import run_job

if __name__ == '__main__':

    input = sys.argv[1]
    access_key = sys.argv[2]
    secret_key = sys.argv[3]

    run_job(input, access_key, secret_key)

    print("Done!")
