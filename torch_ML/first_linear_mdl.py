import torch
import numpy as np
import os
import torch.nn as nn
from luffy._luffy_func import filePath

# part I, define linear mdl as NN
class LinearRegressionModel(nn.Module):
    def __init__(self, input_dim, output_dim):
        super(LinearRegressionModel, self).__init__()
        self.linear = nn.Linear(input_dim, output_dim)

    def forward(self, x):
        out = self.linear(x)
        return out

# part II, define vars & params
x_values = [i for i in range(11)]
x_train = np.array(x_values, dtype=np.float32)
x_train = x_train.reshape(-1, 1)
#x_train.shape
y_values = [2*i + 1 for i in x_values]
y_train = np.array(y_values, dtype=np.float32)
y_train = y_train.reshape(-1, 1)
#y_train.shape
input_dim = 1
output_dim = 1
model = LinearRegressionModel(input_dim, output_dim)
epochs = 1000
learning_rate = 0.01
optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate) ##用随机梯度递减方法来改善参数
criterion = nn.MSELoss() ##损失函数--> 均方误差 sum((x-y)^2)/N

# part III, training start...
for epoch in range(epochs):
    epoch += 1
    # 注意转行成tensor
    inputs = torch.from_numpy(x_train)
    labels = torch.from_numpy(y_train)
    # 梯度要清零每一次迭代
    optimizer.zero_grad()
    # 前向传播
    outputs = model(inputs)
    # 计算损失函数
    loss = criterion(outputs, labels)
    # 返向传播
    loss.backward()
    # 更新权重参数
    optimizer.step()
    if epoch % 10 == 0:
        print('epoch {}, loss {}'.format(epoch, loss.item()))

# part IV, prediction & save mdl.pkl
predicted = model(torch.from_numpy(x_train).requires_grad_()).data.numpy()
predicted

mdlPath = os.path.join(filePath, 'first_linear_model.pkl')
torch.save(model.state_dict(), mdlPath)
model.load_state_dict(torch.load(mdlPath))
