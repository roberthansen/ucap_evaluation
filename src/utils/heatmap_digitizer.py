'''
script copied from:
https://stackoverflow.com/questions/60777750/digitizing-heatmap-and-map-pixels-to-values
'''
import cv2
import numpy as np

# print pixel value on click
def mouse_callback(event, x, y, flags, params):
    if event == cv2.EVENT_LBUTTONDOWN:

        # get specified color
        row = y
        column = x
        color = image[row, column]
        print('color = ', color)

        # calculate range
        thr = 20  # ± color range
        up_thr = color + thr
        up_thr[up_thr < color] = 255
        down_thr = color - thr
        down_thr[down_thr > color] = 0

        # find points in range
        img_thr = cv2.inRange(image, down_thr, up_thr)  # accepted range
        height, width, _ = image.shape
        left_bound = x - (x % round(width/6))
        right_bound = left_bound + round(width/6)
        up_bound = y - (y % round(height/6))
        down_bound = up_bound + round(height/6)
        img_rect = np.zeros((height, width), np.uint8)  # bounded by rectangle
        cv2.rectangle(img_rect, (left_bound, up_bound), (right_bound, down_bound), (255,255,255), -1)
        img_thr = cv2.bitwise_and(img_thr, img_rect)

        # get points around specified point
        img_spec = np.zeros((height, width), np.uint8)  # specified mask
        last_img_spec = np.copy(img_spec)
        img_spec[row, column] = 255
        kernel = np.ones((3,3), np.uint8)  # dilation structuring element
        while cv2.bitwise_xor(img_spec, last_img_spec).any():
            last_img_spec = np.copy(img_spec)
            img_spec = cv2.dilate(img_spec, kernel)
            img_spec = cv2.bitwise_and(img_spec, img_thr)
            cv2.imshow('mask', img_spec)
            cv2.waitKey(10)
        avg = cv2.mean(image, img_spec)[:3]
        print('mean = ', np.around(np.array(avg), 2))
        global avg_table
        avg_table[:, 6 - int(x / (width/6)), 6 - int(y / (height/6))] = avg
        print(avg_table)
    else:
        pass

# average value of each cell in 6x6 matrix
avg_table = np.zeros((3, 6, 6))

# create window and callback
winname = 'img'
cv2.namedWindow(winname)
cv2.setMouseCallback(winname, mouse_callback)

# read & display image
image = cv2.imread('C:\Downloads\supply_cushion_heatmap_2023.png', 1)
cv2.imshow(winname, image)
cv2.waitKey()  # press any key to exit
cv2.destroyAllWindows()