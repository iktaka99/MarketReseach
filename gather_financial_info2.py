# 東証上場銘柄一覧に記載された証券コードの企業の財務指標と財務情報を取得する
# 事前に以下のリンクから東証上場銘柄一覧を取得し、[data_j.xls]のファイル名で保存する
# https://www.jpx.co.jp/markets/statistics-equities/misc/01.html
# 本プログラムと[data_j.xls]を同一フォルダに配置する
# usage: python gather_financial_info.py
# 出力ファイルの一覧
# - company_metrics.csv
#    証券コード,1株当りの配当金,配当利回り,過去5年間の配当利回り平均,配当性向,時価総額
# - company_financial_info.csv
#    証券コード,売上高,純利益,総資産,自己資本比率,自己資本利益率

# 標準ライブラリの読み込み
import os

import numpy as np

# データフレームのライブラリを読み込む
import pandas as pd

# yahooqueryのライブラリを読み込む
from yahooquery import Ticker

# プログレスバーを表示するためのライブラリを読み込む
from tqdm import tqdm


# メイン処理
# 引数:無し
# 戻値:無し
def main():
    # 東証上場銘柄一覧を読み込む
    is_file = os.path.isfile('./data_j2.xls')
    if is_file:
        print('東証上場銘柄一覧を読み込みます。')
        df_data_j = pd.read_excel('./data_j2.xls', index_col=None)
    else:
        print('東証上場銘柄一覧を[data_j2.xls]のファイル名で保存してください。')
        exit()

    # REIT・ベンチャーファンド・カントリーファンド・インフラファンドを除外する
    df_data_j = df_data_j[df_data_j['市場・商品区分'] != 'REIT・ベンチャーファンド・カントリーファンド・インフラファンド']

    # ETF、ETNを除外する
    df_data_j = df_data_j[df_data_j['市場・商品区分'] != 'ETF・ETN']

    # 伊藤園の優先株を除外する
    df_data_j = df_data_j[df_data_j['コード'] != 25935]

    # 企業情報の指標、財務状況を保存するデータフレームを用意する
    df_company_metrics = pd.DataFrame()
    df_company_financial_info = pd.DataFrame()

    # 企業の銘柄名、市場・商品区分、33業種、17業種を保持する
    series_ticker_name = pd.Series()
    series_market_product_category = pd.Series()
    series_type_33 = pd.Series()
    series_type_17 = pd.Series()

    # tickerの企業情報の指標、財務状況を取得する
    for ticker in tqdm(df_data_j['コード']):
        # 証券コードに「.T」を追加する
        df_data_j_filter = df_data_j[df_data_j['コード'] == ticker]
        ticker_num = str(ticker) + '.T'
        ticker_data = Ticker(ticker_num)

        series_ticker_name = pd.concat([series_ticker_name, df_data_j_filter['銘柄名']])
        series_market_product_category = pd.concat([series_market_product_category, df_data_j_filter['市場・商品区分']])
        series_type_33 = pd.concat([series_type_33, df_data_j_filter['33業種区分']])
        series_type_17 = pd.concat([series_type_17, df_data_j_filter['17業種区分']])

        # 企業情報の指標、財務状況を取得する
        df_company_metrics = pd.concat([df_company_metrics, get_company_metrics(ticker_num, ticker_data)])
        df_company_financial_info = pd.concat([df_company_financial_info, get_company_finacial_info(ticker_data)])

    # 銘柄名を追加し、列を並び替える
    df_company_metrics['ticker_name'] = series_ticker_name.values
    df_company_metrics['market_product_category'] = series_market_product_category.values
    df_company_metrics['type_33'] = series_type_33.values
    df_company_metrics['type_17'] = series_type_17.values
    columns = ['ticker', 'ticker_name', 'market_product_category',
               'type_33', 'type_17', 'dividendRate', 'dividendYield',
               'fiveYearAvgDividendYield', 'payoutRatio', 'MarketCap',
               'totalRevenue', 'ROE']
    df_company_metrics = df_company_metrics.reindex(columns=columns)

    # 企業情報の指標、財務状況をCSVファイルに保存する
    df_company_metrics.to_csv('./company_metrics.csv', encoding='cp932', index=False, errors='ignore')
    df_company_financial_info.to_csv('./company_financial_info.csv', encoding='cp932', index=False, errors='ignore')


# 企業の財務指標を取得する
# 引数:証券コード、Tickerオブジェクト
# 戻値:企業の財務指標:Dataframe
def get_company_metrics(ticker_num, ticker_data):
    # 企業の財務指標を保存するリストを用意する
    company_metrics = [ticker_num]

    # summary_detailを用いて1株当りの配当金,配当利回り,過去5年間の配当利回り平均,配当性向,時価総額を取得する
    summary_detail_keys = ['dividendRate', 'dividendYield', 'fiveYearAvgDividendYield', 'payoutRatio', 'marketCap']
    for summary_detail_key in summary_detail_keys:
        try:
            company_metrics.append(ticker_data.summary_detail[ticker_num][summary_detail_key])
        except Exception as e:
            print('証券コード:{},未取得の属性:{}'.format(ticker_num, summary_detail_key))
            company_metrics.append(np.nan)

    # financial_dataを用いて売上高、自己資本利益率を取得する
    financial_data_keys = ['totalRevenue', 'returnOnEquity']
    for financial_data_key in financial_data_keys:
        try:
            company_metrics.append(ticker_data.financial_data[ticker_num][financial_data_key])
        except Exception as e:
            print('証券コード:{},未取得の属性:{}'.format(ticker_num, financial_data_key))
            company_metrics.append(np.nan)

    # 企業の財務指標を保存する
    # 証券コード,1株当りの配当金,配当利回り,過去5年間の配当利回り平均,配当性向,時価総額,売上高,自己資本利益率
    columns = ['ticker', 'dividendRate', 'dividendYield', 'fiveYearAvgDividendYield',
               'payoutRatio', 'MarketCap', 'totalRevenue', 'ROE']
    df_company_metrics = pd.DataFrame(data=[company_metrics], columns=columns)

    return df_company_metrics


# 企業の財務状況を取得する
# 引数:証券コード、Tickerオブジェクト
# 戻値:企業の財務状況:Dataframe
def get_company_finacial_info(ticker_data):
    # income_statement(損益計算書)、cash_flow、balance_sheet(貸借対照表)を取得する
    try:
        income_statement = ticker_data.income_statement(trailing=False)
#        income_statement = ticker_data.income_statement()
        cash_flow = ticker_data.cash_flow(trailing=False)
        balance_sheet = ticker_data.balance_sheet()
    except Exception as e:
        print(e)
        return

    # 過去の売上高、純利益、純資産、総資産を取得する
    try:
        past_totalrevenue = income_statement[['asOfDate', 'TotalRevenue']]
    except Exception as e:
        print(e)
        return        
    try:
        past_grossprofit =  income_statement[['asOfDate', 'GrossProfit']]
    except Exception as e:
        print(e)
        print(income_statement[['asOfDate']])
        income_statement[['GrossProfit']]  = 0
        past_grossprofit =  income_statement[['asOfDate', 'GrossProfit']]

    try:
        past_CostOfRevenue = income_statement[['asOfDate', 'CostOfRevenue']]

        past_DilutedNIAvailtoComStockholders = income_statement[['asOfDate', 'DilutedNIAvailtoComStockholders']]
        past_EBIT =  income_statement[['asOfDate', 'EBIT']]
    except Exception as e:
        print(e)
        return   
    try:
        past_EBITDA =  income_statement[['asOfDate', 'EBITDA']]
    except Exception as e:
        print(e)
#        print("setting EBITDA")
        income_statement[['EBITDA']]  = 0
        past_EBITDA =  income_statement[['asOfDate', 'EBITDA']]
#        print(past_EBITDA)
#        print("done")
    try:
        past_EarningsFromEquityInterest = income_statement[['asOfDate', 'EarningsFromEquityInterest']]
    except Exception as e:
        income_statement[['EarningsFromEquityInterest']]  = 0
        past_EarningsFromEquityInterest = income_statement[['asOfDate', 'EarningsFromEquityInterest']]
    try:     
        past_EarningsFromEquityInterestNetOfTax = income_statement[['asOfDate', 'EarningsFromEquityInterestNetOfTax']]
    except Exception as e:
        income_statement[['EarningsFromEquityInterestNetOfTax']]  = 0
        past_EarningsFromEquityInterestNetOfTax = income_statement[['asOfDate', 'EarningsFromEquityInterestNetOfTax']]
        
    try:
        past_GainOnSaleOfSecurity = income_statement[['asOfDate', 'GainOnSaleOfSecurity']]
    except Exception as e:
        income_statement[['GainOnSaleOfSecurity']]  = 0
        past_GainOnSaleOfSecurity = income_statement[['asOfDate', 'GainOnSaleOfSecurity']]    
        
    try:  
        past_InterestExpense = income_statement[['asOfDate', 'InterestExpense']]
    except Exception as e:
        income_statement[['InterestExpense']]  = 0
        past_InterestExpense = income_statement[['asOfDate', 'InterestExpense']] 
    try: 
        past_InterestExpenseNonOperating = income_statement[['asOfDate', 'InterestExpenseNonOperating']]
    except Exception as e:
        income_statement[['InterestExpenseNonOperating']]  = 0
        past_InterestExpenseNonOperating = income_statement[['asOfDate', 'InterestExpenseNonOperating']]
    try:     
        past_InterestIncome = income_statement[['asOfDate', 'InterestIncome']]
    except Exception as e:
        income_statement[['InterestIncome']]  = 0
        past_InterestIncome = income_statement[['asOfDate', 'InterestIncome']]
    try:   
        past_InterestIncomeNonOperating = income_statement[['asOfDate', 'InterestIncomeNonOperating']]
    except Exception as e:
        income_statement[['InterestIncomeNonOperating']]  = 0   
        past_InterestIncomeNonOperating = income_statement[['asOfDate', 'InterestIncomeNonOperating']]
    try:
        past_MinorityInterests = income_statement[['asOfDate', 'MinorityInterests']]
    except Exception as e:
        income_statement[['MinorityInterests']]  = 0   
        past_MinorityInterests = income_statement[['asOfDate', 'MinorityInterests']]
    try:
        past_NetIncome = income_statement[['asOfDate', 'NetIncome']]
    except Exception as e:
        income_statement[['NetIncome']]  = 0   
        past_NetIncome = income_statement[['asOfDate', 'NetIncome']]
        
    try:   
        past_NetIncomeCommonStockholders = income_statement[['asOfDate', 'NetIncomeCommonStockholders']]
    except Exception as e:
        income_statement[['NetIncomeCommonStockholders']]  = 0   
        past_NetIncomeCommonStockholders = income_statement[['asOfDate', 'NetIncomeCommonStockholders']]
    try:      
        past_NetIncomeContinuousOperations = income_statement[['asOfDate', 'NetIncomeContinuousOperations']]
    except Exception as e:
        income_statement[['NetIncomeContinuousOperations']]  = 0   
        past_NetIncomeContinuousOperations = income_statement[['asOfDate', 'NetIncomeContinuousOperations']]
    try:    
        past_NetIncomeFromContinuingAndDiscontinuedOperation = income_statement[['asOfDate', 'NetIncomeFromContinuingAndDiscontinuedOperation']]
    except Exception as e:
        income_statement[['NetIncomeFromContinuingAndDiscontinuedOperation']]  = 0   
        past_NetIncomeFromContinuingAndDiscontinuedOperation = income_statement[['asOfDate', 'NetIncomeFromContinuingAndDiscontinuedOperation']]
    try:         
        past_NetIncomeIncludingNoncontrollingInterests = income_statement[['asOfDate', 'NetIncomeIncludingNoncontrollingInterests']]
    except Exception as e:
        income_statement[['NetIncomeIncludingNoncontrollingInterests']]  = 0 
        past_NetIncomeIncludingNoncontrollingInterests = income_statement[['asOfDate', 'NetIncomeIncludingNoncontrollingInterests']]
    try:     
        past_NetInterestIncome = income_statement[['asOfDate', 'NetInterestIncome']]
    except Exception as e:
        income_statement[['NetInterestIncome']]  = 0 
        past_NetInterestIncome = income_statement[['asOfDate', 'NetInterestIncome']]
    try: 
        past_NetNonOperatingInterestIncomeExpense = income_statement[['asOfDate', 'NetNonOperatingInterestIncomeExpense']]
    except Exception as e:
        income_statement[['NetNonOperatingInterestIncomeExpense']]  = 0 
        past_NetNonOperatingInterestIncomeExpense = income_statement[['asOfDate', 'NetNonOperatingInterestIncomeExpense']]
    try: 
        past_NormalizedEBITDA = income_statement[['asOfDate', 'NormalizedEBITDA']]
    except Exception as e:
        income_statement[['NormalizedEBITDA']]  = 0 
        past_NormalizedEBITDA = income_statement[['asOfDate', 'NormalizedEBITDA']]
    try: 
        past_NormalizedIncome = income_statement[['asOfDate', 'NormalizedIncome']]
    except Exception as e:
        income_statement[['NormalizedIncome']]  = 0 
        past_NormalizedIncome = income_statement[['asOfDate', 'NormalizedIncome']]
        
    try:
        past_OperatingExpense = income_statement[['asOfDate', 'OperatingExpense']]
    except Exception as e:
        income_statement[['OperatingExpense']]  = 0 
        past_OperatingExpense = income_statement[['asOfDate', 'OperatingExpense']]
    try:
        past_OperatingIncome = income_statement[['asOfDate', 'OperatingIncome']]
    except Exception as e:
        income_statement[['OperatingIncome']]  = 0 
        past_OperatingIncome = income_statement[['asOfDate', 'OperatingIncome']]
    try:       
        past_OperatingRevenue = income_statement[['asOfDate', 'OperatingRevenue']]
    except Exception as e:
        income_statement[['OperatingRevenue']]  = 0 
        past_OperatingRevenue = income_statement[['asOfDate', 'OperatingRevenue']]
    try:        
        past_OtherIncomeExpense = income_statement[['asOfDate', 'OtherIncomeExpense']]
    except Exception as e:
        income_statement[['OtherIncomeExpense']]  = 0 
        past_OtherIncomeExpense = income_statement[['asOfDate', 'OtherIncomeExpense']]
    try: 
        past_OtherNonOperatingIncomeExpenses = income_statement[['asOfDate', 'OtherNonOperatingIncomeExpenses']] 
    except Exception as e:
        income_statement[['OtherNonOperatingIncomeExpenses']]  = 0 
        past_OtherNonOperatingIncomeExpenses = income_statement[['asOfDate', 'OtherNonOperatingIncomeExpenses']] 

    try: 
        past_OtherunderPreferredStockDividend = income_statement[['asOfDate', 'OtherunderPreferredStockDividend']]
    except Exception as e:
        income_statement[['OtherunderPreferredStockDividend']]  = 0 
        past_OtherunderPreferredStockDividend = income_statement[['asOfDate', 'OtherunderPreferredStockDividend']]

    try:     
        past_PretaxIncome = income_statement[['asOfDate', 'PretaxIncome']]
    except Exception as e:
        income_statement[['PretaxIncome']]  = 0 
        past_PretaxIncome = income_statement[['asOfDate', 'PretaxIncome']]

    try:    
        past_ReconciledCostOfRevenue = income_statement[['asOfDate', 'ReconciledCostOfRevenue']]
    except Exception as e:
        income_statement[['ReconciledCostOfRevenue']]  = 0 
        past_ReconciledCostOfRevenue = income_statement[['asOfDate', 'ReconciledCostOfRevenue']]
        
    try:         
        past_ReconciledDepreciation = income_statement[['asOfDate', 'ReconciledDepreciation']]
    except Exception as e:
        income_statement[['ReconciledDepreciation']]  = 0 
        past_ReconciledDepreciation = income_statement[['asOfDate', 'ReconciledDepreciation']]
    try:        
        past_SellingGeneralAndAdministration = income_statement[['asOfDate', 'SellingGeneralAndAdministration']]
    except Exception as e:
        income_statement[['SellingGeneralAndAdministration']]  = 0 
        past_SellingGeneralAndAdministration = income_statement[['asOfDate', 'SellingGeneralAndAdministration']]
        
    try:         
        past_TaxEffectOfUnusualItems = income_statement[['asOfDate', 'TaxEffectOfUnusualItems']]
    except Exception as e:
        income_statement[['TaxEffectOfUnusualItems']]  = 0
        past_TaxEffectOfUnusualItems = income_statement[['asOfDate', 'TaxEffectOfUnusualItems']]      

    try:
        past_TaxProvision = income_statement[['asOfDate', 'TaxProvision']]    
    except Exception as e:
        income_statement[['TaxProvision']]  = 0
        past_TaxProvision = income_statement[['asOfDate', 'TaxProvision']]
        
    try:     
        past_TaxRateForCalcs = income_statement[['asOfDate', 'TaxRateForCalcs']]   
    except Exception as e:
        income_statement[['TaxRateForCalcs']]  = 0     
        past_TaxRateForCalcs = income_statement[['asOfDate', 'TaxRateForCalcs']]   
    try:        
        past_TotalExpenses = income_statement[['asOfDate', 'TotalExpenses']]
    except Exception as e:
        income_statement[['TotalExpenses']]  = 0           
        past_TotalExpenses = income_statement[['asOfDate', 'TotalExpenses']]
        
    try:               
        past_TotalOperatingIncomeAsReported = income_statement[['asOfDate', 'TotalOperatingIncomeAsReported']]
    except Exception as e:
        income_statement[['TotalOperatingIncomeAsReported']]  = 0    
        past_TotalOperatingIncomeAsReported = income_statement[['asOfDate', 'TotalOperatingIncomeAsReported']]
    try:
        past_TotalOtherFinanceCost = income_statement[['asOfDate', 'TotalOtherFinanceCost']] 
    except Exception as e:
        income_statement[['TotalOtherFinanceCost']]  = 0    
        past_TotalOtherFinanceCost = income_statement[['asOfDate', 'TotalOtherFinanceCost']]
    try:
        past_TotalUnusualItems = income_statement[['asOfDate', 'TotalUnusualItems']]
            
    except Exception as e:
        income_statement[['TotalUnusualItems']]  = 0    
        past_TotalUnusualItems = income_statement[['asOfDate', 'TotalUnusualItems']]
    try:
        past_TotalUnusualItemsExcludingGoodwill = income_statement[['asOfDate', 'TotalUnusualItemsExcludingGoodwill']]
            
    except Exception as e:
        income_statement[['TotalUnusualItemsExcludingGoodwill']]  = 0    
        past_TotalUnusualItemsExcludingGoodwill = income_statement[['asOfDate', 'TotalUnusualItemsExcludingGoodwill']]
        
     try:
        past_netincome = cash_flow[['asOfDate', 'NetIncome']]
        past_stockholdersequity = balance_sheet[['asOfDate', 'StockholdersEquity']]
        past_totalassets = balance_sheet[['asOfDate', 'TotalAssets']]
    except Exception as e:
        print(e)
#        print(income_statement.T)
        return

    # income_statement(損益計算書)、cash_flow、balance_sheet(貸借対照表)の決算日を取得する
    past_totalrevenue_asofdate = income_statement['asOfDate']
    past_netincome_asofdate = cash_flow['asOfDate']
    past_stockholdersequity_asofdate = balance_sheet['asOfDate']
    past_totalassets_asofdate = balance_sheet['asOfDate']

    # 損益計算書、キャッシュフロー、貸借対照表で取得できる決算日が異なる場合がある
    # 損益計算書、キャッシュフロー、貸借対照表の決算日が共通の日時を抽出する
    common_asofdate = set(past_totalassets_asofdate).intersection(set(past_netincome_asofdate)).intersection(set(past_stockholdersequity_asofdate)).intersection(set(past_totalrevenue_asofdate))

    # 共通している日時の売上高、純利益、純資産、総資産を抽出する
    result_past_totalrevenue = pd.DataFrame()
    result_past_grossprofit = pd.DataFrame()
    result_past_CostOfRevenue = pd.DataFrame()
    result_past_DilutedNIAvailtoComStockholders =  pd.DataFrame()
    result_past_EBIT =  pd.DataFrame()
    result_past_EBITDA =   pd.DataFrame()
    result_past_EarningsFromEquityInterest =  pd.DataFrame()
    result_past_EarningsFromEquityInterestNetOfTax =  pd.DataFrame()
    result_past_GainOnSaleOfSecurity =  pd.DataFrame()
    result_past_InterestExpense =  pd.DataFrame()
    result_past_InterestExpenseNonOperating =  pd.DataFrame()
    result_past_InterestIncome = pd.DataFrame()
    result_past_InterestIncomeNonOperating =  pd.DataFrame()
    result_past_MinorityInterests =  pd.DataFrame()
    result_past_NetIncome =  pd.DataFrame()
    result_past_NetIncomeCommonStockholders =  pd.DataFrame()
    result_past_NetIncomeContinuousOperations =  pd.DataFrame()
    result_past_NetIncomeFromContinuingAndDiscontinuedOperation =  pd.DataFrame()
    result_past_NetIncomeIncludingNoncontrollingInterests =  pd.DataFrame()
    result_past_NetInterestIncome =  pd.DataFrame()
    result_past_NetNonOperatingInterestIncomeExpense =  pd.DataFrame()
    result_past_NormalizedEBITDA =  pd.DataFrame()
    result_past_NormalizedIncome =  pd.DataFrame()
    result_past_OperatingExpense =  pd.DataFrame()
    result_past_OperatingIncome =  pd.DataFrame()
    result_past_OperatingRevenue =  pd.DataFrame()
    result_past_OtherIncomeExpense =  pd.DataFrame()
    result_past_OtherNonOperatingIncomeExpenses =  pd.DataFrame()
    result_past_OtherunderPreferredStockDividend =  pd.DataFrame()
    result_past_PretaxIncome =  pd.DataFrame()
    result_past_ReconciledCostOfRevenue =  pd.DataFrame()
    result_past_ReconciledDepreciation =  pd.DataFrame()
    result_past_SellingGeneralAndAdministration =  pd.DataFrame()
    result_past_TaxEffectOfUnusualItems =  pd.DataFrame()
    result_past_TaxProvision =  pd.DataFrame()
    result_past_TaxRateForCalcs =  pd.DataFrame()
    result_past_TotalExpenses =  pd.DataFrame()
    result_past_TotalOperatingIncomeAsReported =  pd.DataFrame()
    result_past_TotalOtherFinanceCost =  pd.DataFrame()
    result_past_TotalUnusualItems =  pd.DataFrame()
    result_past_TotalUnusualItemsExcludingGoodwill =  pd.DataFrame()

    result_past_netincome = pd.DataFrame()
    result_past_stockholdersequity = pd.DataFrame()
    result_past_totalassets = pd.DataFrame()

    try:
        for asofdate in common_asofdate:
            result_past_totalrevenue = pd.concat([result_past_totalrevenue, past_totalrevenue[past_totalrevenue['asOfDate'] == asofdate]])
            result_past_grossprofit = pd.concat([result_past_grossprofit, past_grossprofit[past_grossprofit['asOfDate'] == asofdate]])
            result_past_CostOfRevenue = pd.concat([result_past_CostOfRevenue, past_CostOfRevenue[past_CostOfRevenue['asOfDate'] == asofdate]])
            result_past_DilutedNIAvailtoComStockholders =  pd.concat([result_past_DilutedNIAvailtoComStockholders, past_DilutedNIAvailtoComStockholders[past_DilutedNIAvailtoComStockholders['asOfDate'] == asofdate]])
            result_past_EBIT =  pd.concat([result_past_EBIT, past_EBIT[past_EBIT['asOfDate'] == asofdate]])
            result_past_EBITDA =   pd.concat([result_past_EBITDA, past_EBITDA[past_EBITDA['asOfDate'] == asofdate]])
            result_past_EarningsFromEquityInterest =  pd.concat([result_past_EarningsFromEquityInterest, past_EarningsFromEquityInterest[past_EarningsFromEquityInterest['asOfDate'] == asofdate]])
            result_past_EarningsFromEquityInterestNetOfTax =  pd.concat([result_past_EarningsFromEquityInterestNetOfTax, past_EarningsFromEquityInterestNetOfTax[past_EarningsFromEquityInterestNetOfTax['asOfDate'] == asofdate]])
            result_past_GainOnSaleOfSecurity=  pd.concat([result_past_GainOnSaleOfSecurity, past_GainOnSaleOfSecurity[past_GainOnSaleOfSecurity['asOfDate'] == asofdate]])
            result_past_InterestExpense =  pd.concat([result_past_InterestExpense, past_InterestExpense[past_InterestExpense['asOfDate'] == asofdate]])
            result_past_InterestExpenseNonOperating =  pd.concat([result_past_InterestExpenseNonOperating, past_InterestExpenseNonOperating[past_InterestExpenseNonOperating['asOfDate'] == asofdate]])
            result_past_InterestIncome = pd.concat([result_past_InterestIncome, past_InterestIncome[past_InterestIncome['asOfDate'] == asofdate]])
            result_past_InterestIncomeNonOperating =  pd.concat([result_past_InterestIncomeNonOperating, past_InterestIncomeNonOperating[past_InterestIncomeNonOperating['asOfDate'] == asofdate]])
            result_past_MinorityInterests =  pd.concat([result_past_MinorityInterests, past_MinorityInterests[past_MinorityInterests['asOfDate'] == asofdate]])
            result_past_NetIncome =  pd.concat([result_past_NetIncome, past_NetIncome[past_NetIncome['asOfDate'] == asofdate]])
            result_past_NetIncomeCommonStockholders =  pd.concat([result_past_NetIncomeCommonStockholders, past_NetIncomeCommonStockholders[past_NetIncomeCommonStockholders['asOfDate'] == asofdate]])
            result_past_NetIncomeContinuousOperations =  pd.concat([result_past_NetIncomeContinuousOperations, past_NetIncomeContinuousOperations[past_NetIncomeContinuousOperations['asOfDate'] == asofdate]])
            result_past_NetIncomeFromContinuingAndDiscontinuedOperation =  pd.concat([result_past_NetIncomeFromContinuingAndDiscontinuedOperation, past_NetIncomeFromContinuingAndDiscontinuedOperation[past_NetIncomeFromContinuingAndDiscontinuedOperation['asOfDate'] == asofdate]])
            result_past_NetIncomeIncludingNoncontrollingInterests =  pd.concat([result_past_NetIncomeIncludingNoncontrollingInterests, past_NetIncomeIncludingNoncontrollingInterests[past_NetIncomeIncludingNoncontrollingInterests['asOfDate'] == asofdate]])
            result_past_NetInterestIncome =  pd.concat([result_past_NetInterestIncome, past_NetInterestIncome[past_NetInterestIncome['asOfDate'] == asofdate]])
            result_past_NetNonOperatingInterestIncomeExpense =  pd.concat([result_past_NetNonOperatingInterestIncomeExpense, past_NetNonOperatingInterestIncomeExpense[past_NetNonOperatingInterestIncomeExpense['asOfDate'] == asofdate]])
            result_past_NormalizedEBITDA =  pd.concat([result_past_NormalizedEBITDA, past_NormalizedEBITDA[past_NormalizedEBITDA['asOfDate'] == asofdate]])
            result_past_NormalizedIncome =  pd.concat([result_past_NormalizedIncome, past_NormalizedIncome[past_NormalizedIncome['asOfDate'] == asofdate]])
            result_past_OperatingExpense =  pd.concat([result_past_OperatingExpense, past_OperatingExpense[past_OperatingExpense['asOfDate'] == asofdate]])
            result_past_OperatingIncome =  pd.concat([result_past_OperatingIncome, past_OperatingIncome[past_OperatingIncome['asOfDate'] == asofdate]])
            result_past_OperatingRevenue =  pd.concat([result_past_OperatingRevenue, past_OperatingRevenue[past_OperatingRevenue['asOfDate'] == asofdate]])
            result_past_OtherIncomeExpense =  pd.concat([result_past_OtherIncomeExpense, past_OtherIncomeExpense[past_OtherIncomeExpense['asOfDate'] == asofdate]])
            result_past_OtherNonOperatingIncomeExpenses =  pd.concat([result_past_OtherNonOperatingIncomeExpenses, past_OtherNonOperatingIncomeExpenses[past_OtherNonOperatingIncomeExpenses['asOfDate'] == asofdate]])
            result_past_OtherunderPreferredStockDividend =  pd.concat([result_past_OtherunderPreferredStockDividend, past_OtherunderPreferredStockDividend[past_OtherunderPreferredStockDividend['asOfDate'] == asofdate]])
            result_past_PretaxIncome =  pd.concat([result_past_PretaxIncome, past_PretaxIncome[past_PretaxIncome['asOfDate'] == asofdate]])
            result_past_ReconciledCostOfRevenue =  pd.concat([result_past_ReconciledCostOfRevenue, past_ReconciledCostOfRevenue[past_ReconciledCostOfRevenue['asOfDate'] == asofdate]])
            result_past_ReconciledDepreciation =  pd.concat([result_past_ReconciledDepreciation, past_ReconciledDepreciation[past_ReconciledDepreciation['asOfDate'] == asofdate]])
            result_past_SellingGeneralAndAdministration =  pd.concat([result_past_SellingGeneralAndAdministration, past_SellingGeneralAndAdministration[past_SellingGeneralAndAdministration['asOfDate'] == asofdate]])
            result_past_TaxEffectOfUnusualItems =  pd.concat([result_past_TaxEffectOfUnusualItems, past_TaxEffectOfUnusualItems[past_TaxEffectOfUnusualItems['asOfDate'] == asofdate]])
            result_past_TaxProvision =  pd.concat([result_past_TaxProvision, past_TaxProvision[past_TaxProvision['asOfDate'] == asofdate]])
            result_past_TaxRateForCalcs =  pd.concat([result_past_TaxRateForCalcs, past_TaxRateForCalcs[past_TaxRateForCalcs['asOfDate'] == asofdate]])
            result_past_TotalExpenses =  pd.concat([result_past_TotalExpenses, past_TotalExpenses[past_TotalExpenses['asOfDate'] == asofdate]])
            result_past_TotalOperatingIncomeAsReported =  pd.concat([result_past_TotalOperatingIncomeAsReported, past_TotalOperatingIncomeAsReported[past_TotalOperatingIncomeAsReported['asOfDate'] == asofdate]])
            result_past_TotalOtherFinanceCost =  pd.concat([result_past_TotalOtherFinanceCost, past_TotalOtherFinanceCost[past_TotalOtherFinanceCost['asOfDate'] == asofdate]])
            result_past_TotalUnusualItems =  pd.concat([result_past_TotalUnusualItems, past_TotalUnusualItems[past_TotalUnusualItems['asOfDate'] == asofdate]])
            result_past_TotalUnusualItemsExcludingGoodwill =  pd.concat([result_past_TotalUnusualItemsExcludingGoodwill, past_TotalUnusualItemsExcludingGoodwill[past_TotalUnusualItemsExcludingGoodwill['asOfDate'] == asofdate]])


            result_past_netincome = pd.concat([result_past_netincome, past_netincome[past_netincome['asOfDate'] == asofdate]])
            result_past_stockholdersequity = pd.concat([result_past_stockholdersequity, past_stockholdersequity[past_stockholdersequity['asOfDate'] == asofdate]])
            result_past_totalassets = pd.concat([result_past_totalassets, past_totalassets[past_totalassets['asOfDate'] == asofdate]])
    except Exception as e:
        print(e)
        return
    
    # 時刻で昇順に並び替える
    result_past_totalrevenue = result_past_totalrevenue.sort_values('asOfDate')
    result_past_grossprofit = result_past_grossprofit.sort_values('asOfDate')
    result_past_CostOfRevenue = result_past_CostOfRevenue.sort_values('asOfDate')
    result_past_DilutedNIAvailtoComStockholders = result_past_DilutedNIAvailtoComStockholders.sort_values('asOfDate')
    result_past_EBIT = result_past_EBIT.sort_values('asOfDate')
    result_past_EBITDA = result_past_EBITDA.sort_values('asOfDate')
    result_past_EarningsFromEquityInterest = result_past_EarningsFromEquityInterest.sort_values('asOfDate')
    result_past_EarningsFromEquityInterestNetOfTax = result_past_EarningsFromEquityInterestNetOfTax.sort_values('asOfDate')
    result_past_GainOnSaleOfSecurity = result_past_GainOnSaleOfSecurity.sort_values('asOfDate')
    result_past_InterestExpense = result_past_InterestExpense.sort_values('asOfDate')
    result_past_InterestExpenseNonOperating = result_past_InterestExpenseNonOperating.sort_values('asOfDate')
    result_past_InterestIncome = result_past_InterestIncome.sort_values('asOfDate')
    result_past_InterestIncomeNonOperating = result_past_InterestIncomeNonOperating.sort_values('asOfDate')
    result_past_MinorityInterests = result_past_MinorityInterests.sort_values('asOfDate')
    result_past_NetIncome = result_past_NetIncome.sort_values('asOfDate')
    result_past_NetIncomeCommonStockholders = result_past_NetIncomeCommonStockholders.sort_values('asOfDate')
    result_past_NetIncomeContinuousOperations = result_past_NetIncomeContinuousOperations.sort_values('asOfDate')
    result_past_NetIncomeFromContinuingAndDiscontinuedOperation = result_past_NetIncomeFromContinuingAndDiscontinuedOperation.sort_values('asOfDate')
    result_past_NetIncomeIncludingNoncontrollingInterests = result_past_NetIncomeIncludingNoncontrollingInterests.sort_values('asOfDate')
    result_past_NetInterestIncome = result_past_NetInterestIncome.sort_values('asOfDate')
    result_past_NetNonOperatingInterestIncomeExpense = result_past_NetNonOperatingInterestIncomeExpense.sort_values('asOfDate')
    result_past_NormalizedEBITDA = result_past_NormalizedEBITDA.sort_values('asOfDate')
    result_past_NormalizedIncome = result_past_NormalizedIncome.sort_values('asOfDate')
    result_past_OperatingExpense = result_past_OperatingExpense.sort_values('asOfDate')
    result_past_OperatingIncome = result_past_OperatingIncome.sort_values('asOfDate')
    result_past_OperatingRevenue = result_past_OperatingRevenue.sort_values('asOfDate')
    result_past_OtherIncomeExpense = result_past_OtherIncomeExpense.sort_values('asOfDate')
    result_past_OtherNonOperatingIncomeExpenses = result_past_OtherNonOperatingIncomeExpenses.sort_values('asOfDate')
    result_past_OtherunderPreferredStockDividend = result_past_OtherunderPreferredStockDividend.sort_values('asOfDate')
    result_past_PretaxIncome = result_past_PretaxIncome.sort_values('asOfDate')
    result_past_ReconciledCostOfRevenue = result_past_ReconciledCostOfRevenue.sort_values('asOfDate')
    result_past_ReconciledDepreciation = result_past_ReconciledDepreciation.sort_values('asOfDate')
    result_past_SellingGeneralAndAdministration = result_past_SellingGeneralAndAdministration.sort_values('asOfDate')
    result_past_TaxEffectOfUnusualItems = result_past_TaxEffectOfUnusualItems.sort_values('asOfDate')
    result_past_TaxProvision = result_past_TaxProvision.sort_values('asOfDate')
    result_past_TaxRateForCalcs = result_past_TaxRateForCalcs.sort_values('asOfDate')
    result_past_TotalExpenses = result_past_TotalExpenses.sort_values('asOfDate')
    result_past_TotalOperatingIncomeAsReported = result_past_TotalOperatingIncomeAsReported.sort_values('asOfDate')
    result_past_TotalOtherFinanceCost = result_past_TotalOtherFinanceCost.sort_values('asOfDate')
    result_past_TotalUnusualItems = result_past_TotalUnusualItems.sort_values('asOfDate')
    result_past_TotalUnusualItemsExcludingGoodwill = result_past_TotalUnusualItemsExcludingGoodwill.sort_values('asOfDate')

    result_past_netincome = result_past_netincome.sort_values('asOfDate')
    result_past_stockholdersequity = result_past_stockholdersequity.sort_values('asOfDate')
    result_past_totalassets = result_past_totalassets.sort_values('asOfDate')

    # 過去の自己資本比率を計算する
    ticker_past_capitaladequacyratio = result_past_stockholdersequity['StockholdersEquity'] / result_past_totalassets['TotalAssets']

    # 過去の自己資本利益率を計算する
    ticker_past_roe = result_past_netincome['NetIncome'] / result_past_stockholdersequity['StockholdersEquity']

    # 企業情報の過去の指標を保存する
    # 過去の売上高,純利益,総資産,自己資本比率,自己資本利益率
    df_financial_info = pd.DataFrame({'asOfDate':               result_past_totalrevenue['asOfDate'],
                                      'TotalRevenue':           result_past_totalrevenue['TotalRevenue'],
                                      'GrossProfit':            result_past_grossprofit['GrossProfit'],
                                      'CostOfRevenue':          result_past_CostOfRevenue['CostOfRevenue'],
                                      'DilutedNIAvailtoComStockholders':    result_past_DilutedNIAvailtoComStockholders['DilutedNIAvailtoComStockholders'],
                                      'EBIT':                   result_past_EBIT['EBIT'],
                                      'EBITDA':                 result_past_EBITDA['EBITDA'],
                                      'InterestExpense':        result_past_InterestExpense['InterestExpense'],
                                      'InterestExpenseNonOperating':        result_past_InterestExpenseNonOperating['InterestExpenseNonOperating'],
                                      'InterestIncome':         result_past_InterestIncome['InterestIncome'],
                                      'InterestIncomeNonOperating':         result_past_InterestIncomeNonOperating['InterestIncomeNonOperating'],
                                      'NetIncomeCommonStockholders':        result_past_NetIncomeCommonStockholders['NetIncomeCommonStockholders'],
                                      'NetIncomeContinuousOperations':      result_past_NetIncomeContinuousOperations['NetIncomeContinuousOperations'],
                                      'NetIncomeFromContinuingAndDiscontinuedOperation':    result_past_NetIncomeFromContinuingAndDiscontinuedOperation['NetIncomeFromContinuingAndDiscontinuedOperation'],
                                      'NetIncomeIncludingNoncontrollingInterests':          result_past_NetIncomeIncludingNoncontrollingInterests['NetIncomeIncludingNoncontrollingInterests'],
                                      'NetInterestIncome':      result_past_NetInterestIncome['NetInterestIncome'],
                                      'NetNonOperatingInterestIncomeExpense':   result_past_NetNonOperatingInterestIncomeExpense['NetNonOperatingInterestIncomeExpense'],
                                      'NormalizedEBITDA':       result_past_NormalizedEBITDA['NormalizedEBITDA'],
                                      'NormalizedIncome':       result_past_NormalizedIncome['NormalizedIncome'],
                                      'OperatingExpense':       result_past_OperatingExpense['OperatingExpense'],
                                      'OperatingIncome':        result_past_OperatingIncome['OperatingIncome'],
                                      'OperatingRevenue':       result_past_OperatingRevenue['OperatingRevenue'],
                                      'OtherNonOperatingIncomeExpenses':    result_past_OtherNonOperatingIncomeExpenses['OtherNonOperatingIncomeExpenses'],
                                      'PretaxIncome':   result_past_PretaxIncome['PretaxIncome'],
                                      'ReconciledCostOfRevenue':    result_past_ReconciledCostOfRevenue['ReconciledCostOfRevenue'],
                                      'ReconciledDepreciation':     result_past_ReconciledDepreciation['ReconciledDepreciation'],
                                      'TaxEffectOfUnusualItems':    result_past_TaxEffectOfUnusualItems['TaxEffectOfUnusualItems'],
                                      'TaxProvision':   result_past_TaxProvision['TaxProvision'],
                                      'TaxRateForCalcs':    result_past_TaxRateForCalcs['TaxRateForCalcs'],
                                      'TotalExpenses':  result_past_TotalExpenses['TotalExpenses'],
                                      'TotalOperatingIncomeAsReported': result_past_TotalOperatingIncomeAsReported['TotalOperatingIncomeAsReported'],
                                      'TotalUnusualItems':  result_past_TotalUnusualItems['TotalUnusualItems'],
                                      'TotalUnusualItemsExcludingGoodwill': result_past_TotalUnusualItemsExcludingGoodwill['TotalUnusualItemsExcludingGoodwill'],

                                      'StockholdersEquity':     result_past_stockholdersequity['StockholdersEquity'],
                                      'TotalAssets':            result_past_totalassets['TotalAssets'],
                                      'capitalAdequacyRatio':   ticker_past_capitaladequacyratio,
                                      'ROE':                    ticker_past_roe})

    df_financial_info = df_financial_info.reset_index()
#    print(df_financial_info)
    return df_financial_info


if __name__ == "__main__":
    main()
