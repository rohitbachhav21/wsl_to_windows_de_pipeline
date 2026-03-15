def load_last_id():
    try:
        with open("checkpoint.txt") as f:
            return int(f.read().strip())
    except:
        return 0

def save_last_id(id):
    with open("checkpoint.txt", "w") as f:
        f.write(str(id))

last_id = load_last_id()