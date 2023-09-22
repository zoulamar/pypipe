#!/usr/bin/echo Script not meant as executable:

"""
Some sort of reusable PyPlot config.
Copyright 2022 zoulamar@fel.cvut.cz

Licensed under the "THE BEER-WARE LICENSE" (Revision 42):
zoulamar@fel.cvut.cz wrote this file. As long as you retain this notice you
can do whatever you want with this stuff. If we meet some day, and you think
this stuff is worth it, you can buy me a beer or coffee in return

Inspired by these:
- https://github.com/kubikji2/scientifig (CC-0)
- https://stackoverflow.com/questions/30079590/use-matplotlib-color-map-for-color-cycle
- https://matplotlib.org/stable/tutorials/intermediate/color_cycle.html
- https://tex.stackexchange.com/questions/391074/how-to-use-the-siunitx-package-within-python-matplotlib

Features:
- [x] One nice rcfile for my diploma thesis.
- [ ] Enable siunitx package... Tried but did not work.
"""

from typing import List, Tuple, Union
import matplotlib.pyplot as plt
import numpy as np
import colorsys

# size relevant constants
IEEE_SINGLE_WIDTH = 7.16 #inches
IEEE_COL_WIDTH = 3.5 #inches
IEEE_FIG_SIZE = (IEEE_COL_WIDTH, IEEE_COL_WIDTH) #modify for text width cols

CM = 1/2.54
TEXTWIDTH=(21-3-2.4)*CM
FIG_SIZE=(TEXTWIDTH, TEXTWIDTH/2)

#class PageContext:
#    def __init__(self, format:str) -> None:
#        pass

def subplots(size_mult:Union[None,List,Tuple,np.ndarray]=None, size_square_frac:Union[None,int]=None, size_exact:Union[Tuple[float,float],None]=None, *args, **kwargs) -> Tuple[plt.Figure, plt.Axes]:
    """ Wrapper around `plt.subplots` which creates the figure size in units relative to `FIG_SIZE` """
    setcount = sum(map(lambda x : x is not None, [size_mult, size_square_frac, size_exact]))
    assert setcount <= 1
    if size_mult is not None:
        figsize = size_mult[0] * FIG_SIZE[0], size_mult[1] * FIG_SIZE[1]
    elif size_square_frac is not None:
        a = TEXTWIDTH/size_square_frac
        figsize = (a, a)
    elif size_exact is not None:
        figsize = size_exact
    else:
        figsize = FIG_SIZE
    fig, ax = plt.subplots(figsize=figsize, *args, **kwargs)
    assert isinstance(ax, plt.Axes)
    return  fig, ax

def hcv_colors(ncolors = 10, base_color = '#005eb8'):
    """ Creates linearly spaced discrete color cycler. """
    if isinstance(base_color, str):
        if base_color.startswith("#") and len(base_color) == 7:
            r = int(base_color[1:3], 16) / 2**8
            g = int(base_color[3:5], 16) / 2**8
            b = int(base_color[5:7], 16) / 2**8
            h, s, v = colorsys.rgb_to_hsv(r,g,b)
        elif base_color.startswith("0x") and len(base_color) == 8:
            r = int(base_color[2:4], 16) / 2**8
            g = int(base_color[4:6], 16) / 2**8
            b = int(base_color[6:8], 16) / 2**8
            h, s, v = colorsys.rgb_to_hsv(r,g,b)
        else:
            raise ValueError("Base color not undestood.")
    else:
        raise ValueError("Base color not undestood.")
    h_step = 1/ncolors
    rgbs = []
    for i in range(ncolors):
        h_this = h + i*h_step
        s_this = s
        v_this = v
        if h_this > 1:
            h_this -= 1
        rgbs.append(colorsys.hsv_to_rgb(h_this, s_this, v_this))
    #print(np.array(rgbs) * 2**8)
    return rgbs

font = {
    "font.family": "serif",
    "axes.labelsize": 8,
    "font.size": 10,
    "legend.fontsize": 8,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    # Blank entries should cause plots to inherit fonts from the document
    "font.serif": [],
    "font.sans-serif": [],
    "font.monospace": [],
}

pdflatex = {
    "text.usetex": True,
    "text.latex.preamble": r"\usepackage{siunitx}",
}

plots = {
    "figure.figsize": FIG_SIZE,
    "axes.prop_cycle": plt.cycler('color', plt.get_cmap('Dark2').colors),
    "figure.autolayout": True, # equals tight layout
    "axes.grid": True,
    "legend.framealpha": 1,
}

#CTU_CYCLE_10 = hcv_colors(10, "0x005eb8")
#CTU_CYCLE_5 = hcv_colors(5, "0x005eb8")

plt.rcParams.update(font)
plt.rcParams.update(pdflatex)
plt.rcParams.update(plots)

def align_yaxis(ax1, v1, ax2, v2):
    """adjust ax2 ylimit so that v2 in ax2 is aligned to v1 in ax1"""
    _, y1 = ax1.transData.transform((0, v1))
    _, y2 = ax2.transData.transform((0, v2))
    adjust_yaxis(ax2,(y1-y2)/2,v2)
    adjust_yaxis(ax1,(y2-y1)/2,v1)

def adjust_yaxis(ax,ydif,v):
    """shift axis ax by ydiff, maintaining point v at the same location"""
    inv = ax.transData.inverted()
    _, dy = inv.transform((0, 0)) - inv.transform((0, ydif))
    miny, maxy = ax.get_ylim()
    miny, maxy = miny - v, maxy - v
    if -miny>maxy or (-miny==maxy and dy > 0):
        nminy = miny
        nmaxy = miny*(maxy+dy)/(miny+dy)
    else:
        nmaxy = maxy
        nminy = maxy*(miny+dy)/(maxy+dy)
    ax.set_ylim(nminy+v, nmaxy+v)
