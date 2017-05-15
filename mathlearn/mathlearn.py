# -*- coding:utf-8 -*-
import matplotlib.pyplot as plt
import numpy as np

w = np.linspace(0.1, 1000, 1000)
p = np.abs(1 / (1 + 0.1j * w))

plt.subplot(221)  # m表示是图排成m行，n表示图排成n列,p表示图所在的位置
plt.plot(w, p, linewidth=2)  # linewidth线宽=2
plt.ylim(0, 1.5)

plt.subplot(222)  # m表示是图排成m行，n表示图排成n列,p表示图所在的位置
plt.semilogx(w, p, linewidth=2)  # semilogx: x轴为对数刻度，y轴为线性刻度
plt.ylim(0, 1.5)

plt.subplot(223)  # m表示是图排成m行，n表示图排成n列,p表示图所在的位置
plt.semilogy(w, p, linewidth=2)  # y轴为对数坐标系
plt.ylim(0, 1.5)

plt.subplot(224)  # m表示是图排成m行，n表示图排成n列,p表示图所在的位置
plt.loglog(w, p, linewidth=2)  # 双对数坐标系
plt.ylim(0, 1.5)

plt.show()
