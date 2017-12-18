import os

if __name__=="__main__":
    path="F:/data/test"
    for root, dirs, files in os.walk(path):
        for dir in dirs:
            for file in os.listdir(path+'/'+dir):
                with open(path + '/' + dir + '_new', "a+") as new_f:
                    with open(path + '/' + dir + '/' + file, "r") as f:
                        str=""
                        for line in f:
                            str=str+line.strip()+" "
                        str=str.strip()
                        str=dir+" "+file+" "+str+"\n"
                        new_f.write(str)



