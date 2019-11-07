# Abstract image generator using random functions
# Nathan Reed, July 2012

import uuid, io
import numpy as np, optparse, random, time
from PIL import Image

# Parse command-line options
parser = optparse.OptionParser()
parser.add_option('-o', '--output', dest='outputPath', help='Write output to FILE', metavar='FILE')
parser.add_option('-d', '--dims', dest='dims', default='512x512', help='Image width x height, e.g. 320x240')
parser.add_option('-s', '--seed', dest='seed', default=int(1000 * time.time()), help='Random seed (uses system time by default)')
options, _ = parser.parse_args()

dX, dY = (int(n) for n in options.dims.lower().split('x'))
try:
	options.seed = int(options.seed)
except ValueError:
	pass
random.seed(options.seed)
if not options.outputPath:
	options.outputPath = str(options.seed) + '.png'

# Generate x and y images, with 3D shape so operations will correctly broadcast.
xArray = np.linspace(0.0, 1.0, dX).reshape((1, dX, 1))
yArray = np.linspace(0.0, 1.0, dY).reshape((dY, 1, 1))

# Adaptor functions for the recursive generator
# Note: using python's random module because numpy's doesn't handle seeds longer than 32 bits.
def randColor(): return np.array([random.random(), random.random(), random.random()]).reshape((1, 1, 3))
def xVar(): return xArray
def yVar(): return yArray
def safeDivide(a, b): return np.divide(a, np.maximum(b, 0.001))

# Recursively build an image using a random function.  Functions are built as a parse tree top-down,
# with each node chosen randomly from the following list.  The first element in each tuple is the
# number of required recursive calls and the second element is the function to evaluate the result.
functions = (
		(0, randColor),
		(0, xVar),
		(0, yVar),
		(1, np.sin),
		(1, np.cos),
		(2, np.add),
		(2, np.subtract),
		(2, np.multiply),
		(2, safeDivide),
	)

depthMin = 2
depthMax = 10


def build_img(depth = 0):
	funcs = [f for f in functions if
				(f[0] > 0 and depth < depthMax) or
				(f[0] == 0 and depth >= depthMin)]
	nArgs, func = random.choice(funcs)
	args = [build_img(depth + 1) for n in range(nArgs)]
	return func(*args)


def save_img():
	img = build_img()

	# Ensure it has the right dimensions
	img = np.tile(img, (dX // img.shape[0], dY // img.shape[1], 3 // img.shape[2]))

	# Convert to 8-bit, send to PIL and save
	img8Bit = np.uint8(np.rint(img.clip(0.0, 1.0) * 255.0))
	output_path = str(uuid.uuid1()) + '.png'

	imgByteArr = io.BytesIO()
	Image.fromarray(img8Bit).save(imgByteArr, format='PNG')
	imgByteArr = imgByteArr.getvalue()

	return imgByteArr

	#Image.fromarray(img8Bit).save(output_path)
	#print('Seed %s; wrote output to %s' % (repr(options.seed), output_path))
