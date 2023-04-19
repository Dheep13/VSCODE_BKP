import glob 
import os   



def readFileImages(strFolderName):
    st = os.path.join(strFolderName, "*.gif")
    return glob.glob(st)

print (readFileImages(os.getcwd()) )