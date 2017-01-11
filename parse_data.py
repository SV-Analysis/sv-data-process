

def read_data():
    with open('data/results_hk.csv') as f:
        line = f.readline()
        num = 10
        total = 0
        while line:
            line = f.readline()
            total += 1
            if total % 10000 == 0:
                print total
        print total

if __name__ == "__main__":
    read_data()