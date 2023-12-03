import sys

WORD_COUNT = 1_000_000
KEY_LEN = 18

if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} [DATASET_FILE]")

filepath = sys.argv[1]

dictionary = set()

prev_len = 0
counter = 0
with open(filepath, "r") as f:
    while True:
        line = f.readline()
        if line == "":
            break
        line = line[0:KEY_LEN]
        if line in dictionary:
            print(f"{line} - is duplicated!!!")
        else:
            dictionary.add(line)
        
        #if prev_len != len(line):
        #    print(f"length changed from {prev_len} to {len(line)}")
        #prev_len = len(line)

        counter += 1
        if counter > WORD_COUNT:
            break

min_key = min(dictionary)
max_key = max(dictionary)
for ch in min_key:
    print(ord(ch), end=" ")
print()
for ch in max_key:
    print(ord(ch), end=" ")
print()