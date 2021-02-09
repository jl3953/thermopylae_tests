# ARG1 = input csv file
# ARG2 = output png file
# ARG3 = x axis name
# ARG4 = y axis name
# ARG5 = name of x column to plot
# ARG6 = name of y column to plot

set terminal png

set xlabel ARG3
set ylabel ARG4
set output ARG2

plot ARG1 using ARG5:ARG6 with linespoint
