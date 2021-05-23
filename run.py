import sys
from chord import ChordNode

def main(m:int):
    c = ChordNode(m)
    c.run()
    commands = {
        "join": c.join,
        "lookup": c.lookup,
        "predecessor":c.find_predecessor,
        "successor":c.find_succesor,
        "exit": c.exit,
        "ft":print,
        "ns":print,
        "ids":print
    }
    while True:
        try:
            usr_input = input().split()
            arg_0 = usr_input[0]
            try:
                command = commands[arg_0]
            except KeyError:
                print("Unrecognized command")
                continue

            try:
                args = []
                if arg_0 == "join" and len(usr_input) > 1:
                    args.append(usr_input[1])
                elif arg_0 == 'ft':
                    args.append(c.finger_table)
                elif arg_0 == 'ns':
                    args.append(c.node_set)
                elif arg_0 == 'ids':
                    args.append(c.ip_router_table)
                elif arg_0 != "exit" and len(usr_input) > 1:
                    args.append(int(usr_input[1]))
            except IndexError:
                print("Bad Arguments")
                continue
            
            print(command(*args))
            if arg_0 == "exit":
                break
        
        except KeyboardInterrupt:
            print("Exit")
            c.joined = False
            c.online = False
            exit(1)




if __name__ == "__main__":
    arg = sys.argv
    if len(arg) == 1:
        m = 5
    else:
        m = int(arg[1])
    main(m)