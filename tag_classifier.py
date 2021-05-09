# -*- coding: utf-8 -*-
"""

"""
def rule1(candleRow, classScore,ruleScore):
    if candleRow['todayOpen'] > (candleRow['prevDayClose'] + 
                                    0.1 * candleRow['atr']):
        classScore['score'] += 1.0
        ruleScore[0] += 1.0
    elif candleRow['todayOpen'] < (candleRow['prevDayClose'] - 
                                    0.1 * candleRow['atr']):
        classScore['score'] -= 1.0
        ruleScore[0] -= 1.0
    else:
        classScore['score'] -= 0.0

def rule2_hepler(high, low, close, candleRow, classScore,ruleScore):
        percentage = None
        try:
            percentage = (close-low)/(high-low)
        except ZeroDivisionError:
            percentage = 1.0
        if percentage >= 0.8:
            classScore['score'] += 1.0
            ruleScore[1] += 1.0
        elif percentage >= 0.7:
            classScore['score'] += 0.5
            ruleScore[1] += 0.5
        elif percentage > 0.3:
            classScore['score'] += 0.0
        elif percentage > 0.2:
            classScore['score'] -= 0.5
            ruleScore[1] -= 0.5
        else:
            classScore['score'] -= 1.0
            ruleScore[1] -= 1.0
            
def rule2(candleRow, classScore,ruleScore):
    [high, low, close] = candleRow['firstFiveMinuteOhlc'][1:]    
    pc = candleRow['prevDayClose']

    if candleRow['firstFiveMinuteOhlc'][2] > pc: low = pc
    if candleRow['firstFiveMinuteOhlc'][1] < pc: high = pc
    rule2_hepler(high, low, close, candleRow, classScore,ruleScore)

    [high, low, close] = candleRow['secondFiveMinuteOhlc'][1:]
    rule2_hepler(high, low, close, candleRow, classScore,ruleScore)

    [high, low, close] = candleRow['thirdFiveMinuteOhlc'][1:]
    rule2_hepler(high, low, close, candleRow, classScore,ruleScore)


def rule3_helper(cc1, cc2, atr, candleRow, classScore,ruleScore):
    if cc2 > cc1 + 0.03 * atr:
        classScore['score'] += 1.0
        ruleScore[2]+=1.0
    elif cc2 < cc1 - 0.03 * atr:
        classScore['score'] -= 1.0
        ruleScore[2]-=1.0

    else:
        classScore['score'] += 0.0
        ruleScore[2]+=0.0
    
def rule3(candleRow, classScore,ruleScore):
    [cc1, cc2, cc3, atr] = [candleRow['firstFiveMinuteOhlc'][-1], candleRow['secondFiveMinuteOhlc'][-1], 
                            candleRow['thirdFiveMinuteOhlc'][-1], candleRow['atr']] 
    rule3_helper(cc1, cc2, atr, candleRow, classScore,ruleScore)
    rule3_helper(cc2, cc3, atr, candleRow, classScore,ruleScore)
    rule3_helper(cc1, cc3, atr, candleRow, classScore,ruleScore)
         
def rule4_helper(pc, cc, atr, candleRow, classScore,ruleScore):
    if cc > pc + 0.05 * atr:
        classScore['score'] += 1.0
        ruleScore[3]+=1.0
    elif cc < pc - 0.05 * atr:
        classScore['score'] -= 1.0
        ruleScore[3]-=1.0
    else:
        classScore['score'] += 0.0
        ruleScore[3]+=0.0

def rule4(candleRow, classScore,ruleScore):
    [cc1, cc2, cc3, atr, pc] = [candleRow['firstFiveMinuteOhlc'][-1], candleRow['secondFiveMinuteOhlc'][-1], 
                                candleRow['thirdFiveMinuteOhlc'][-1], candleRow['atr'], candleRow['prevDayClose']]
    rule4_helper(pc, cc1, atr, candleRow, classScore,ruleScore)
    rule4_helper(pc, cc2, atr, candleRow, classScore,ruleScore)
    rule4_helper(pc, cc3, atr, candleRow, classScore,ruleScore)

def get_class(context, symbol):
    candleRow = context.candleDf.loc[symbol, :]
    classScore = {'score': 0.0}
    ruleScore=[0,0,0,0]
    rule1(candleRow, classScore, ruleScore)
    rule2(candleRow, classScore, ruleScore)
    rule3(candleRow, classScore, ruleScore)
    rule4(candleRow, classScore, ruleScore)
    
    if classScore['score'] >=2.0:
        return  classScore['score'], "Bullish", ruleScore
    elif classScore['score'] <= -2.0:
        return classScore['score'], "Bearish", ruleScore
    else:
        return classScore['score'], "Neutral", ruleScore
