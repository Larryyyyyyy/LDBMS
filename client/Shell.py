import os
class Shell(object):
    def __init__(self, client):
        self.client = client
    
    def run(self):
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
