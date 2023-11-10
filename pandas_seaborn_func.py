import numpy as np
import pandas as pd
import re, os, json, shutil
from matplotlib import pyplot as plt
import seaborn as sns
from luffy._luffy_func import filePath


tada = pd.DataFrame(np.random.randint(0,100, size=(100,3)), columns=['jdm','jed','kin'])




def heatmap(data, x_label, y_label, style, ax_title, cbar_label=''):

    plt.rcParams['font.sans-serif'] = 'SimHei'
    fig, ax = plt.subplots(figsize=[7, 7])
    fig.set_facecolor('0.97')
    im = ax.imshow(data, cmap = style)
    # Show all ticks and label them with the respective list entries
    x_ticks = ax.set_xticks(np.arange(len(x_label)))  # 设置刻度
    y_ticks = ax.set_yticks(np.arange(len(y_label)))  # 设置刻度
    x_labels = ax.set_xticklabels(x_label, rotation=30, fontsize='small', rotation_mode="anchor", ha="right")
    # 替换刻度标签, ha 是label 在TICK 的位置
    y_labels = ax.set_yticklabels(y_label, fontsize=7.5)
    # 替换刻度标签
    # Loop over data dimensions and create text annotations.
    for i in range(len(y_label)):
        for j in range(len(x_label)):
            text = ax.text(j, i, int(data.iloc[i, j]),
                           ha="center", va="center", color="w", fontsize=7)
    ax.set_title(ax_title)
    if len(cbar_label) > 2:
        #cbar = ax.figure.colorbar(im, ax=ax, shrink=0.5)
        cbar = plt.colorbar(im, ax=ax, shrink=0.5)
        cbar.ax.set_ylabel(cbar_label, rotation=-90, va="bottom")
    #fig.tight_layout()
    plt.show()

    return


def horizontal_bar_chart(data, category, count):

    plt.rcParams['font.sans-serif'] = 'SimHei'
    fig, ax = plt.subplots(figsize=[6, 5])
    fig.set_facecolor('0.97')
    y_pos = np.arange(len(data[category]))
    error = [0] * len(data[category])
    #hbars = ax.barh(y=y_pos, width=data[count], height=0.5, xerr=error, align='center')
    ax = sns.barplot(data=data, x= count, y= category, orient='h', color='olivedrab', saturation= 0.3)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(data[category].tolist(), fontsize=7.5)
    #ax.invert_yaxis()  # inverse the category labels
    ax.set_xlabel('user count')
    ax.set_title('AARRR')
    # ax.bar_label(hbars, fmt='%.2f')
    ax.set_xlim(right=max(data[count]) * 1.3)  # adjust xlim to fit labels
    rects = ax.patches  # get the pos of ticks
    bar_labels = data[count].tolist()
    for rect, label in zip(rects, bar_labels):
        ax.text(
            rect.get_width() + 5, rect.get_y() + 0.5 * rect.get_height()
            , label
            , ha="center", va="bottom", color='m'
        )
    plt.show()

    return


def plot_line_chart(dataDict):

    # dataDict = {'label': 'ydata', 'x':'xdata, 'xlabel': xlabel, 'ylabel'; ylabel, 'type':", 'nbins': }
    ## plot plot
    # dataDict = daDict

    x = dataDict['x']
    daDict = {k: v for k, v in dataDict.items() if k not in ('x','xlabel', 'ylabel', 'type', 'nbins')}
    legendList = [key for key in daDict.keys()]
    xlabel = dataDict['xlabel']
    ylabel = dataDict['ylabel']
    draw_type = dataDict['type']
    plt.rcParams['font.sans-serif'] = 'SimHei'
    fig, ax = plt.subplots(figsize = [8,8])   # , layout = 'constrained')
    fig.set_facecolor('0.97')
    # super title
    fig.suptitle('这里是主标题', fontsize=14)
    # 利用text属性添加副标题
    fig.text(0.45, 0.9, '这是副标题')
    ax.set_facecolor('0.85')
    if draw_type == 'plot':
        for label, bItem in daDict.items():
            ax.plot(x, bItem, label=label, marker='o', markersize=2.5)
        max_v = int(max(daDict[legendList[len(legendList) - 1]]))
    elif draw_type == 'scatter':
        for label, bItem in daDict.items():
            ax.scatter(x, bItem, label=label, marker='o')
        max_v = int(max(daDict[legendList[len(legendList) - 1]]))
    elif draw_type == 'hist':
        colors = ['orange']
        n_bins = int(dataDict['nbins'])
        n, bins, patches = ax.hist(x, n_bins, density=False, histtype='bar', color=colors, label=colors)
        for i in range(len(n)):
            ax.text(bins[i], n[i] * 1.02, int(n[i]), fontsize=12, horizontalalignment="center")
        max_v = max(x)
    elif draw_type == 'pie':
        sizes = [round(item/sum(x), 2) for item in x]
        labels = daDict['labels']
        ax.pie(sizes, labels=labels, autopct='%1.1f%%')

    ax.set(#ylim=[1, max_v * 2]
           #, xticks=x
           #, yticks=[i for i in range(0, max_v * 2, 1000000)]
            xlabel=xlabel
           , ylabel=ylabel
           )
    ax.grid(axis='y')   ##  only keep y grid
    ax.legend(legendList)
    fig.tight_layout()
    plt.savefig(os.path.join(filePath, f'fig_{xlabel}_{ylabel}.png'))
    plt.show()

    return plt


