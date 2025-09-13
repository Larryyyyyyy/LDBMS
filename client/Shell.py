from client.Client import Client

class Shell(object):
    def __init__(self, client: Client):
        self.client = client
    
    def run(self) -> None:
        while True:
            stat_str = input(":> ")
            if stat_str in ["exit", "quit"]:
                break
            try:
                res = self.client.execute(stat_str.encode("UTF-8"))
                print(res.decode("UTF-8"))
            except Exception as e:
                print(e)
        self.client.close()
