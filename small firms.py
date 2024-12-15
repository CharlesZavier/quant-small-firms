# 导入函数库
from jqdata import *
from jqfactor import get_factor_values
import numpy as np
import tushare as ts
import pandas as pd
import os
from datetime import datetime, timedelta

#0 初始化函数，设定基准等等
def initialize(context):
    # 设定中证500作为基准
    set_benchmark('000905.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 打开防未来函数
    set_option("avoid_future_data", True)
    # 将滑点设置为0
    set_slippage(FixedSlippage(0))
    # 设置买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱，不同滑点影响可在归因分析中查看
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003,
                             close_today_commission=0, min_commission=5), type='stock')
    # 过滤掉order系列API产生的比error级别低的log
    log.set_level('order', 'error')

    # 初始化全局变量
    g.stock_num = 10  # 持仓股票数量10
    g.limit_days = 20  # 卖出后20天不再操作
    g.limit_up_list = []
    g.hold_list = []
    g.history_hold_list = []
    g.not_buy_again_list = []
    # 增加动态仓位管理
    g.position_ratio = 1.0  # 初始满仓
    run_daily(adjust_position_ratio, time='14:30')

    # 运行函数，每天运行
    # 每天早上9:05分准备目标股票清单
    run_daily(prepare_stock_list, time='9:05', reference_security='000300.XSHG')
    # 每周一早上9:40分调仓
    run_weekly(weekly_adjustment, weekday=1, time='9:40', reference_security='000300.XSHG')
    # 每天检查涨停，对涨停的股票不做处理
    run_daily(check_limit_up, time='14:00', reference_security='000300.XSHG')
    run_daily(print_position_info, time='15:10', reference_security='000300.XSHG')

 # 初始化Tushare Pro API
    pro = ts.pro_api('976be784e4aa16a57a16f753d1791246043561ddaf0c67e2a4509750')


class TushareDataHandler:
    @staticmethod
    def get_all_securities(types=['stock']):
        """
        获取所有股票代码
        替代 get_all_securities
        """
        df = pro.stock_basic(exchange='', list_status='L',
                             fields='ts_code,symbol,name')
        return df['ts_code']

    @staticmethod
    def get_price(stock, count=1, end_date=None, frequency='daily', fields=['close']):
        """
        获取股票价格数据
        替代 get_price
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=count * 2)).strftime('%Y%m%d')

        df = pro.daily(ts_code=stock, start_date=start_date, end_date=end_date)

        # 根据需要的字段返回数据
        result_df = df[fields]
        result_df.index = pd.to_datetime(df['trade_date'])
        return result_df

    @staticmethod
    def get_current_data(stock_list=None):
        """
        获取当前股票数据
        替代 get_current_data
        """
        if stock_list is None:
            stock_list = TushareDataHandler.get_all_securities()

        # 获取最新日线数据
        df = pro.daily(ts_code=','.join(stock_list), limit=1)

        # 构造类似 get_current_data 的返回结果
        current_data = {}
        for stock in stock_list:
            stock_df = df[df['ts_code'] == stock]
            if not stock_df.empty:
                current_data[stock] = {
                    'name': stock,
                    'paused': False,  # Tushare没有直接的停牌信息
                    'is_st': stock_df['ts_code'].str.contains('ST').any(),
                    'high_limit': stock_df['high'].values[0] * 1.1,
                    'low_limit': stock_df['low'].values[0] * 0.9
                }
        return current_data

    @staticmethod
    def get_fundamentals(query, date=None):
        """
        获取基本面数据
        替代 get_fundamentals
        """
        # 这里需要根据具体查询条件实现
        # Tushare提供了多个基本面数据接口
        df = pro.daily_basic(ts_code='', trade_date=date)
        return df

#1 数据缓存至本地
class SmartDataCache:
    def __init__(self, cache_dir='./stock_data_cache', max_cache_days=30):
        self.cache_dir = cache_dir
        self.max_cache_days = max_cache_days
        os.makedirs(cache_dir, exist_ok=True)

        # 初始化Tushare
        self.pro = ts.pro_api('your_token')

    def get_daily_data(self, stock_code, start_date, end_date):
        cache_file = os.path.join(self.cache_dir, f'{stock_code}_daily.csv')

        # 检查缓存文件是否存在和是否过期
        if os.path.exists(cache_file):
            cached_df = pd.read_csv(cache_file, parse_dates=['trade_date'])
            latest_cached_date = cached_df['trade_date'].max()

            # 如果缓存不是最新的，更新数据
            if latest_cached_date < pd.to_datetime(end_date):
                new_data = self.pro.daily(
                    ts_code=stock_code,
                    start_date=latest_cached_date.strftime('%20210101'),
                    end_date=end_date
                )
                # 合并新老数据
                df = pd.concat([cached_df, new_data]).drop_duplicates(subset=['trade_date'])
                df.to_csv(cache_file, index=False)
                return df
            else:
                return cached_df
        else:
            # 如果没有缓存，直接获取数据
            df = self.pro.daily(
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            df.to_csv(cache_file, index=False)
            return df

    def get_stock_list(self, force_update=False):
        cache_file = os.path.join(self.cache_dir, 'stock_list.csv')
        current_date = datetime.now()

        # 检查缓存是否存在且在有效期内
        if os.path.exists(cache_file) and not force_update:
            cache_modified = datetime.fromtimestamp(os.path.getmtime(cache_file))
            if (current_date - cache_modified).days < self.max_cache_days:
                return pd.read_csv(cache_file)

        # 更新股票列表
        stock_list = self.pro.stock_basic(
            exchange='',
            list_status='L',
            fields='ts_code,symbol,name,area,industry,list_date'
        )
        stock_list.to_csv(cache_file, index=False)
        return stock_list

    def clean_old_cache(self):
        """
        清理超过max_cache_days的缓存文件
        """
        current_date = datetime.now()
        for filename in os.listdir(self.cache_dir):
            file_path = os.path.join(self.cache_dir, filename)
            file_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
            if (current_date - file_modified).days > self.max_cache_days:
                os.remove(file_path)


# 使用示例
cache = SmartDataCache()

# 获取股票列表
stocks = cache.get_stock_list()

# 获取某只股票的日线数据
stock_data = cache.get_daily_data('000001.SZ', '20230101', '20231231')

# 定期清理缓存
cache.clean_old_cache()



# 2-1 回测因子性能
def backtest_factor_performance(factor_list, start_date, end_date):
    """
    回测一组因子在特定时间段的表现

    :param factor_list: 待回测的因子列表
    :param start_date: 回测开始日期
    :param end_date: 回测结束日期
    :return: 因子及其对应的收益率
    """
    factor_performance = {}

    # 获取所有可交易股票
    all_stocks = list(get_all_securities(['stock']).index)

    for factor in factor_list:
        try:
            # 检查因子是否存在
            try:
                factor_values = get_factor_values(
                    all_stocks,
                    factor,
                    start_date=start_date,
                    end_date=end_date
                )

                # 如果因子值为空，跳过这个因子
                if factor_values[factor].empty:
                    log.error(f"因子 {factor} 在指定日期范围内无数据")
                    continue
            except Exception as e:
                log.error(f"获取因子 {factor} 失败: {e}")
                continue

            # 根据因子值选择股票（前20%）
            daily_returns = []
            for date in factor_values[factor].index:
                daily_factor = factor_values[factor].loc[date]
                # 去除NaN值
                daily_factor = daily_factor.dropna()

                if len(daily_factor) > 0:
                    # 选择因子值最高的20%股票
                    top_stocks = daily_factor.nlargest(int(len(daily_factor) * 0.2)).index.tolist()

                    # 获取这些股票的收益率
                    try:
                        stock_returns = get_price(
                            top_stocks,
                            start_date=date,
                            end_date=date,
                            frequency='daily',
                            fields='close'
                        )['close'].pct_change()

                        daily_returns.append(stock_returns.mean())
                    except Exception as e:
                        log.error(f"获取股票 {top_stocks} 价格失败: {e}")

            # 计算平均收益率
            if daily_returns:
                avg_return = np.mean(daily_returns)
                factor_performance[factor] = avg_return
                log.info(f"因子 {factor} 平均收益率: {avg_return}")

        except Exception as e:
            log.error(f"回测因子 {factor} 完全失败: {e}")

    return factor_performance


# 2-2 动态选择最优因子
def select_best_factor(factor_list, end_date, lookback_days=60):
    """
    选择最近时间段内表现最好的因子

    :param factor_list: 备选因子列表
    :param end_date: 结束日期
    :param lookback_days: 回测回溯天数
    :return: 最优因子
    """
    start_date = get_trade_days(end_date=end_date, count=lookback_days)[0]

    factor_performance = backtest_factor_performance(factor_list, start_date, end_date)

    # 如果没有可用因子，返回默认因子
    if not factor_performance:
        log.warning("没有可用因子，使用默认因子 roa_ttm_8y")
        return 'roa_ttm_8y'

    # 选择收益率最高的因子
    best_factor = max(factor_performance, key=factor_performance.get)

    log.info(f"最优因子: {best_factor}, 收益率: {factor_performance[best_factor]}")

    return best_factor


# 3-1 选股模块
def get_factor_filter_list(context, stock_list, jqfactor, sort, p1, p2):
    yesterday = context.previous_date
    score_list = get_factor_values(stock_list, jqfactor, end_date=yesterday, count=1)[jqfactor].iloc[0].tolist()
    df = pd.DataFrame(columns=['code', 'score'])
    df['code'] = stock_list
    df['score'] = score_list
    df = df.dropna()
    df.sort_values(by='score', ascending=sort, inplace=True)
    filter_list = list(df.code)[int(p1 * len(df)):int(p2 * len(df))]
    return filter_list


# 3-2 选股模块
def get_stock_list(context):
    yesterday = context.previous_date
    # 使用Tushare获取股票列表
    all_stocks = list(TushareDataHandler.get_all_securities())
    # 预定义可选择的因子
    factor_list = [
        'roa_ttm_8y',  # 资产回报率
        'roe_ttm',  # 净资产收益率
        'net_profit_yoy',  # 净利润同比增长率
        'eps_ttm',  # 每股收益
        'gross_profit_margin',  # 毛利率
    ]
    # 动态选择最优因子
    best_factor = select_best_factor(factor_list, context.previous_date)

    # 定义财务指标筛选的filters
    filters = [
        (best_factor, False)  # 使用动态选择的最优因子
    ]

    yesterday = context.previous_date

    # 初始股票池筛选
    initial_list = list(get_all_securities().index)

    # 过滤股票
    initial_list = filter_kcbj_stock(initial_list)  # 过滤科创板
    initial_list = filter_st_stock(initial_list)  # 过滤ST股
    initial_list = filter_new_stock(context, initial_list, 250)  # 上市满250天

    # 综合筛选
    filtered_stocks = []
    for factor, ascending in filters:
        try:
            scores = get_factor_values(initial_list, factor, end_date=yesterday, count=1)[factor].iloc[0]
            df = pd.DataFrame({'code': initial_list, 'score': scores})
            df = df.dropna().sort_values('score', ascending=ascending)

            # 选择前30%的股票
            filtered_stocks.extend(list(df['code'][:int(len(df) * 0.3)]))
        except Exception as e:
            log.error(f"筛选因子 {factor} 失败: {e}")

    # 去重
    filtered_stocks = list(set(filtered_stocks))

    # 市值筛选：选择小市值股票
    q = query(
        valuation.code,
        valuation.circulating_market_cap
    ).filter(
        valuation.code.in_(filtered_stocks),
        valuation.circulating_market_cap > 10,  # 大于10亿
        valuation.circulating_market_cap < 100  # 小于100亿
    ).order_by(
        valuation.circulating_market_cap.asc()  # 按市值升序
    ).limit(30)  # 限制30只股票

    df = get_fundamentals(q, date=yesterday)
    final_list = list(df['code'])

    # 波动率控制
    volatility_list = []
    for stock in final_list:
        try:
            price_data = get_price(stock, count=60, end_date=yesterday, frequency='daily', fields=['close'])
            daily_returns = price_data['close'].pct_change().dropna()
            volatility = daily_returns.std() * np.sqrt(252)  # 年化波动率

            # 波动率控制在30%以内
            if volatility < 0.6:
                volatility_list.append(stock)
        except:
            continue

    return volatility_list[:10]  # 最终返回10只股票

# 3-3 准备服票池
def prepare_stock_list(context):
    # 获取已持有列表
    g.hold_list = []
    for position in list(context.portfolio.positions.values()):
        stock = position.security
        g.hold_list.append(stock)
    # 获取最近一段时闻持有过的服票列表
    g.history_hold_list.append(g.hold_list)
    if len(g.history_hold_list) >= g.limit_days:
        g.history_hold_list = g.history_hold_list[-g.limit_days:]
    temp_set = set()
    for hold_list in g.history_hold_list:
        for stock in hold_list:
            temp_set.add(stock)
    g.not_buy_again_list = list(temp_set)
    # 获取昨日涨停列表
    if g.hold_list != []:
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close', 'high_limit'],
                       count=1, panel=False, fill_paused=False)
        df = df[df['close'] == df['high_limit']]
        g.high_limit_list = list(df.code)
    else:
        g.high_limit_list = []

# 4-1 根据市场趋势动态调整仓位
def adjust_position_ratio(context):
    benchmark_returns = \
    get_price('000905.XSHG', count=20, end_date=context.previous_date, frequency='daily', fields=['close'])[
        'close'].pct_change()

    # 市场趋势判断
    if np.mean(benchmark_returns[-5:]) < 0:
        g.position_ratio = 0.5  # 市场下跌时降低仓位
    else:
        g.position_ratio = 1.0  # 市场上涨时满仓

  # 简单择时：大盘趋势判断
    benchmark_returns = \
    get_price('000905.XSHG', count=60, end_date=context.previous_date, frequency='daily', fields=['close'])[
        'close'].pct_change()
    # 平均收益为负时，暂不调仓
    if np.mean(benchmark_returns[-20:]) < 0:
        log.info("市场趋势不佳，暂不调仓")
        return


# 4-2 整体调整持仓
def weekly_adjustment(context):
    # 获取应买入列表，过滤掉停牌、涨跌停
    target_list = get_stock_list(context)
    # 如果没有目标股票，直接返回
    if not target_list:
        log.warning("没有找到符合条件的股票")
        return

    target_list = filter_paused_stock(target_list)
    target_list = filter_limitup_stock(context, target_list)
    target_list = filter_limitdown_stock(context, target_list)

    # 10和选出的股票数量取较小值
    target_list = target_list[:min(g.stock_num, len(target_list))]

    # 增加仓位控制
    cash = context.portfolio.cash
    total_value = context.portfolio.total_value

    # 如果目标股票列表为空，直接返回
    if not target_list:
        log.warning("经过过滤后没有可买入的股票")
        return

    # 根据动态仓位比例调整
    value_per_stock = (total_value * g.position_ratio) / len(target_list)

    # 调仓卖出（卖出不是涨停并且也不在targetlist上的股票）
    for stock in g.hold_list:
        if (stock not in target_list) and (stock not in g.high_limit_list):
            log.info("卖出[%s]" % (stock))
            position = context.portfolio.positions[stock]
            close_position(position)
        else:
            log.info("已持有[%s]" % (stock))

    # 调仓买入
    position_count = len(context.portfolio.positions)
    target_num = len(target_list)
    if target_num > position_count:  # 如果目标数量大于持仓数量
        value = context.portfolio.cash / (target_num - position_count)  # 平均分配手里的现金
        for stock in target_list:  # 对targetlist中的股票执行下列循环
            if context.portfolio.positions[stock].total_amount == 0:
                if open_position(stock, value):
                    if len(context.portfolio.positions) == target_num:
                        break


# 5-1 调整昨日涨停股票
def check_limit_up(context):
    now_time = context.current_dt
    if g.high_limit_list != []:
        for stock in g.high_limit_list:
            current_data = get_price(stock, end_date=now_time, frequency='1m', fields=['close', 'high_limit'],
                                     skip_paused=False, fq='pre', count=1, panel=False, fill_paused=True)
            if current_data.iloc[0, 0] < current_data.iloc[0, 1]:
                log.info("[%s]涨停打开，卖出" % (stock))
                position = context.portfolio.positions.get(stock)
                if position:  # 检查 position 是否为 None
                    close_position(position)
            else:
                log.info("[%s]涨停，继续持有" % (stock))

            # 2-1 过滤停牌股票


def filter_paused_stock(stock_list):
    current_data = get_current_data()
    return [stock for stock in stock_list if not current_data[stock].paused]


# 5-2 过滤st及其他具有退市标签的股票
def filter_st_stock(stock_list):
    current_data = get_current_data()
    return [stock for stock in stock_list
            if not current_data[stock].is_st
            and 'sT' not in current_data[stock].name
            and '*' not in current_data[stock].name
            and '退' not in current_data[stock].name]


# 5-3 获取最近N个交易日内有涨停的股票
def get_recent_limit_up_stock(context, stock_list, recent_days):
    stat_date = context.previous_date
    new_list = []
    for stock in stock_list:
        df = get_price(stock, end_date=stat_date, frequency='daily', fields=['close', 'high_limit'], count=recent_days,
                       panel=False, fill_paused=False)
        df = df[df['close'] == df['high_limit']]
        if len(df) > 0:
            new_list.append(stock)

    return new_list


# 5-4 过滤涨停的服票
def filter_limitup_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)
    current_data = get_current_data()
    return [stock for stock in stock_list if stock in context.portfolio.positions or (
                stock in current_data and last_prices[stock][-1] < current_data[stock].high_limit)]


# 5-5 过滤跌停的服票
def filter_limitdown_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)
    current_data = get_current_data()
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
            or last_prices[stock][-1] > current_data[stock].low_limit]


# 5-6 过滤科创北交股票
def filter_kcbj_stock(stock_list):
    for stock in stock_list[:]:
        if stock[0] == '4' or stock[0] == '8' or stock[:2] == '68':
            stock_list.remove(stock)
        return stock_list


# 5-7 过滤次新股
def filter_new_stock(context, stock_list, d):
    yesterday = context.previous_date
    return [stock for stock in stock_list if
            not yesterday - get_security_info(stock).start_date < datetime.timedelta(days=d)]


# 6-1 交易模块-自定义下单
def order_target_value_(security, value):
    if value == 0:
        log.debug("selling out %s" % (security))
    else:
        log.debug("order %s to value %f" % (security, value))
    return order_target_value(security, value)


# 6-2 交易模块-开仓
def open_position(security, value):
    order = order_target_value_(security, value)
    if order != None and order.filled > 0:
        return True
    return False


# 6-3 交易模块-平仓
def close_position(position):
    security = position.security
    order = order_target_value_(security, 0)  # 可能因停牌失败
    if order != None:
        if order.status == OrderStatus.held and order.filled == order.amount:
            return True
    return False


# 6-4 交易模块-调仓
def adjust_position(context, buy_stocks, stock_num):
    for stock in context.portfolio.positions:
        if stock not in buy_stocks:
            log.info("[%s]不在应买入列表中" % (stock))
            position = context.portfolio.positions[stock]
            close_position(position)
        else:
            log.info("[%s]已经持有无需重复买入" % (stock))

    positioncount = len(context.portfolio.positions)
    if stock_num > position_count:
        value = context.portfolio.cash / (stock_num - position_count)
        for stock in buy_stocks:
            if context.portfolio.positions[stock].total_amount == 0:
                if open_position(stock, value):
                    if len(context.portfolio.positions) == stock_num:
                        break


# 7-1 打印每日持仓信息
def print_position_info(context):
    # 打印当天成交记录
    trades = get_trades()
    for _trade in trades.values():
        print("成交记录：" + str(_trade))  # 修正了prit为print
    # 打印账户信息
    for position in list(context.portfolio.positions.values()):
        securities = position.security
        cost = position.avg_cost
        price = position.value
        ret = 100 * (price / cost - 1)
        value = position.value
        amount = position.total_amount
        print('代码：{}'.format(securities))
        print('成本价：{}'.format(format(cost, '.2f')))
        print('现价：{}'.format(price))
        print('收益率：{}%'.format(format(ret, '.2f')))
        print('持仓（股）：{}'.format(amount))
        print('市值：{}'.format(format(value, '.2f')))
        print('----------------------------------')
    print('----------------------------------分割线----------------------------------')



