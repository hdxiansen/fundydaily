import backtrader as bt
import akshare as ak
import pandas as pd

# 定义策略（与前例相同）
class SMACrossover(bt.Strategy):
    params = (
        ('short_period', 5),   # 短期均线周期
        ('long_period', 20),    # 长期均线周期
    )

    def __init__(self):
        self.sma_short = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.short_period)
        self.sma_long = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.long_period)

    def next(self):
        if not self.position:
            if self.sma_short[0] > self.sma_long[0] and self.sma_short[-1] <= self.sma_long[-1]:
                self.buy()
        else:
            if self.sma_short[0] < self.sma_long[0] and self.sma_short[-1] >= self.sma_long[-1]:
                self.sell()

# 通过 AKShare 获取沪深300指数数据
def fetch_data():
    # 获取沪深300历史行情
    df = ak.index_zh_a_hist(symbol="000300", period="daily", start_date="20240901", end_date="20250321")
    
    # 转换日期格式为 datetime，并设置为索引
    df['日期'] = pd.to_datetime(df['日期'])
    df.set_index('日期', inplace=True)
    
    # 按时间升序排列（Backtrader要求数据按时间升序）
    df.sort_index(ascending=True, inplace=True)
    
    # 重命名列以匹配 Backtrader 的要求
    df.rename(columns={
        '开盘': 'open',
        '最高': 'high',
        '最低': 'low',
        '收盘': 'close',
        '成交量': 'volume'
    }, inplace=True)
    print(df)
    return df

# 获取数据
data_df = fetch_data()

# 将数据转换为 Backtrader 的格式
data = bt.feeds.PandasData(dataname=data_df)

# 初始化回测引擎
cerebro = bt.Cerebro()
cerebro.adddata(data)          # 添加数据
cerebro.addstrategy(SMACrossover)  # 添加策略
cerebro.broker.setcash(100000.0)   # 初始资金（10万元）
cerebro.broker.setcommission(commission=0.001)  # 佣金 0.1%

# 打印初始资金
print('初始资金: %.2f' % cerebro.broker.getvalue())

# 运行回测
cerebro.run()

# 打印最终资金
print('最终资金: %.2f' % cerebro.broker.getvalue())

# 可视化回测结果
cerebro.plot()