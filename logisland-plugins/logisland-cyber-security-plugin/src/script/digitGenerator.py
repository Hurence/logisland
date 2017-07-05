from Tkinter import *
import numpy as np
import math
import glob
import os
import time
import argparse
from array import *

from scipy import ndimage
from PIL import Image, ImageDraw, ImageFilter
import PIL.ImageOps
import PIL.ImageTk
import cv2

from kafka import KafkaProducer

# this script required openCV, PIL, Tkinter, scipy and numpy libraries

def getBestShift(img):
    cy, cx = ndimage.measurements.center_of_mass(img)

    rows, cols = img.shape
    shiftx = np.round(cols / 2.0 - cx).astype(int)
    shifty = np.round(rows / 2.0 - cy).astype(int)

    return shiftx, shifty


def shift(img, sx, sy):
    rows, cols = img.shape
    M = np.float32([[1, 0, sx], [0, 1, sy]])
    shifted = cv2.warpAffine(img, M, (cols, rows))
    return shifted


# this is the first method i developped I used to generate the MNIST like image
# it gave worst result that current reformatImg method
def AltReformatImg(argv):
    im = Image.open(argv).convert('L')
    width = float(im.size[0])
    height = float(im.size[1])
    newImage = Image.new('L', (28, 28), (255)) #creates white canvas of 28x28 pixels

    if width > height:
        nheight = int(round((20.0/width*height),0)) #resize
        if nheight == 0: #rare case but minimum is 1 pixel
            nheight = 1
        # resize and sharpen
        img = im.resize((20,nheight), Image.ANTIALIAS).filter(ImageFilter.SHARPEN)
        wtop = int(round(((28 - nheight)/2),0)) #caculate horizontal position
        newImage.paste(img, (4, wtop)) #paste resized image on white canvas
        invertedImage = PIL.ImageOps.invert(newImage) #invert image to have a black background
    else:
        nwidth = int(round((20.0/height*width),0)) #resize
        if (nwidth == 0):
            nwidth = 1
            # resize and sharpen
        img = im.resize((nwidth,20), Image.ANTIALIAS).filter(ImageFilter.SHARPEN)
        wleft = int(round(((28 - nwidth)/2),0)) #caculate vertical pozition
        newImage.paste(img, (wleft, 4)) #paste resized image on white canvas
        invertedImage = PIL.ImageOps.invert(newImage) #invert image to have a black background
    invertedImage.save("/tmp/1_tmpimg.png");


def reformatImg(path):
    gray = cv2.imread(path, cv2.IMREAD_GRAYSCALE)
    global photo
    # if blank image then return
    if gray.min() == gray.max():
        return

    cv2.imwrite("0_" + path, gray)
    (thresh, gray) = cv2.threshold(gray, 128, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)
    #gray = cv2.bitwise_not(gray)
    cv2.imwrite("00_" + path, gray)
    # resize the images and invert it (black background)
    gray = cv2.resize(255 - gray, (28, 28))
    # save the processed images
    while np.sum(gray[0]) == 0:
        gray = gray[1:]

    while np.sum(gray[:, 0]) == 0:
        gray = np.delete(gray, 0, 1)

    while np.sum(gray[-1]) == 0:
        gray = gray[:-1]

    while np.sum(gray[:, -1]) == 0:
        gray = np.delete(gray, -1, 1)
    cv2.imwrite("2_" + path, gray)
    rows, cols = gray.shape

    if rows > cols:
        factor = 20.0 / rows
        rows = 20
        cols = int(round(cols * factor))
        gray = cv2.resize(gray, (cols, rows))
    else:
        factor = 20.0 / cols
        cols = 20
        rows = int(round(rows * factor))
        gray = cv2.resize(gray, (cols, rows))

    colsPadding = (int(math.ceil((28 - cols) / 2.0)), int(math.floor((28 - cols) / 2.0)))
    rowsPadding = (int(math.ceil((28 - rows) / 2.0)), int(math.floor((28 - rows) / 2.0)))
    gray = np.lib.pad(gray, (rowsPadding, colsPadding), 'constant')
    shiftx, shifty = getBestShift(gray)
    shifted = shift(gray, shiftx, shifty)
    gray = shifted

    cv2.imwrite("/tmp/1_tmpimg.png", gray)
    im = Image.open(r'/tmp/1_tmpimg.png')
    photo = PIL.ImageTk.PhotoImage(im)
    #label = Label(master, image=photo)
    #label.pack()
    w.create_image((50, 50), image=photo, anchor='nw')



def paint(event):
    python_black = "#000000"
    x1, y1 = (event.x - 1), (event.y - 1)
    x2, y2 = (event.x + 1), (event.y + 1)
    w.create_oval(x1 - ep, y1 - ep, x2 + ep, y2 + ep, fill=python_black)


def featuredImg(path):
    Im = Image.open(path)
    pixel = Im.load()
    width, height = Im.size
    data_image = array('B')

    for x in range(0, width):
        for y in range(0, height):
            data_image.append(pixel[y, x])

    hexval = "{0:#0{1}x}".format(1, 6)

    header = array('B')
    header.extend([0, 0, 8, 1, 0, 0])
    header.append(int('0x' + hexval[2:][:2], 16))
    header.append(int('0x' + hexval[2:][2:], 16))
    if max([width, height]) <= 256:
        header.extend([0, 0, 0, width, 0, 0, 0, height])
    else:
        raise ValueError('Image exceeds maximum size: 256x256 pixels');

    header[3] = 3  # Changing MSB for image data (0x00000803)

    data_image = header + data_image

    output_file = open(path + '-images-idx3-ubyte', 'wb')
    data_image.tofile(output_file)
    output_file.close()
    return(data_image)


def formatAndSend():
    for f in glob.glob("/tmp/tmpimg*.png"):
        os.remove(f)

    img = Image.open("/tmp/tmpimg.ps")
    img.save("/tmp/tmpimg.png", "png")
    reformatImg("/tmp/tmpimg.png")
    data_image = featuredImg("/tmp/1_tmpimg.png")

    # send featured image to kafka
    producer = KafkaProducer(bootstrap_servers=broker)
    producer.send(topic, data_image.tostring())
    time.sleep(2)

def _save():
    w.postscript(file='/tmp/tmpimg.ps')
    try:
        formatAndSend()
    except Exception as inst:
        print("Error during processing. Check your drawing",inst)


def _quit():
    master.destroy()


def _erase():
    w.delete("all")


def clean():
    for f in glob.glob("/tmp/*tmpimg*"):
        os.remove(f)


# Main function

#command args parser
parser = argparse.ArgumentParser(description='Example with long option names')
parser.add_argument('--topic', help="kafka topic", action="store", dest="t", required=True)
parser.add_argument('--broker', help="Comma separated broker list", action="store", dest="b", required=True)
arguments = parser.parse_args()

# global variable
topic=arguments.t
broker=arguments.b

canvas_width = 400
canvas_height = 500

white = (255, 255, 255)
black = (0, 0, 0)
ep = 40
photo = 0
# Initialize drawing window
master = Tk()
master.title("Digit Drawing Area")
image1 = Image.new("RGB", (canvas_width, canvas_height), white)
draw = ImageDraw.Draw(image1)

w = Canvas(master,
           width=canvas_width,
           height=canvas_height)

w.pack(expand=YES, fill=BOTH)
w.bind("<B1-Motion>", paint)
frame = Frame(master, bg='lightgrey', width=400, height=40)
frame.pack(fill='x')
button1 = Button(frame, text='Send to kafka', command=_save)
button1.pack(side='left', padx=10)
button2 = Button(frame, text='Clear', command=_erase)
button2.pack(side='left', padx=10)
button3 = Button(frame, text='Exit', command=_quit)
button3.pack(side='left', padx=10)
message = Label(master, text="Draw a digit")
message.pack(side=BOTTOM)

mainloop()

# clean tmp files before leaving
clean()

