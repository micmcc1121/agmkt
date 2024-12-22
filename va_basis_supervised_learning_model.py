'''
Creates supervised learning model to estimate
prices in Virginia ag commodity markets using
linear regression ML supervised learning
approaches taught in Datacamp course.

created by Michael McConnell
craeted on April 21, 2024
'''

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import agmkt_pkg.nass as nass
import agmkt_pkg.env_vars as env_vars

# For ML analysis
from sklearn.linear_model import LinearRegression, Ridge, Lasso 
from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
import seaborn as sns

# create sql connection
user = env_vars.mktdb_user()
password = env_vars.mktdb_passwd()
server = env_vars.mktdb_server()

connection_string = f'postgresql://{user}:{password}@{server}/mktdb'
eng = create_engine(connection_string)
conn = eng.connect()

# Pull AMS Data

va_price_qry = '''
select report_end_date, commodity, trade_loc, delivery_start, avg_price, "basis Min", "basis Max", "basis Min Futures Month", "freight", "trans_mode", "current"
from mars_grain_bids
where commodity in ('Corn')
    and market_location_state in ('VA')
    -- and "trade_loc" in ('Norfolk Terminal Area')
    and "trade_loc" in ('VA Eastern Shore Area')
    -- and "grade" in ('US #1')
    and "current" in ('Yes')
    and "freight" in ('Delivered')
    and trans_mode in ('Truck')
order by report_end_date
;
'''

va_price_df = pd.read_sql(va_price_qry, con=conn)
va_price_df['basis_mid'] = (va_price_df['basis Min'] + va_price_df['basis Max']) / 2
va_basis = va_price_df[['report_end_date', 'basis_mid']].set_index('report_end_date').rename(columns={'basis_mid':'VA'})

mid_data = '''
select report_end_date, market_location_state, trade_loc, delivery_point, avg_price, "basis Min", "basis Max", "basis Min Futures Month"
from mars_grain_bids
where commodity in ('Corn')
    and "current" in ('Yes')
    and (
    (market_location_state in ('IL')
        and trade_loc in ('Central')
        and delivery_point in ('Mills and Processors')
    )
    or (market_location_state in ('OR')
        and trade_loc in ('Gulf Coast Ports - LA')
        and delivery_point in ('Export Elevators')
    )
    or (market_location_state in ('IA')
        and trade_loc in ('Southwest')
        and delivery_point in ('Terminal Elevators')
    )
    or (market_location_state in ('OH')
        and trade_loc in ('Ohio River')
        and delivery_point in ('Barge Loading Elevators')
    )
    )
order by report_end_date, market_location_state
;
'''
reg_price_df = pd.read_sql(mid_data, con=conn)
reg_price_df['basis_mid'] = (reg_price_df['basis Min'] + reg_price_df['basis Max']) / 2
reg_basis = reg_price_df.pivot_table(index='report_end_date', columns='market_location_state', values='basis_mid')

# Create Dataframe
df = va_basis.merge(reg_basis, left_index=True, right_index=True, how='left') #.asfreq('D')
for state in ['IA', 'IL', 'OR', 'OH']:
    for l in np.arange(4, 9):
        col_name = str(state) + '_' + str(l)
        df[str(col_name)] = df[str(state)].shift(1)

df = df.resample('W-FRI').mean().ffill().dropna()
df.to_csv('./raw_data/va_basis_mdl_df.csv', index=True)

fig = plt.subplot()
sns.lineplot(data=va_basis, x=va_basis.index.dayofyear, y='VA', hue=va_basis.index.year.astype('category'))
plt.title('Seasonal Chart of VA Soybean Basis')
plt.show()

# Develop ML Models

y = df[['VA']].values
X = df.iloc[:, 5:].values

# Regression Prediction
reg = LinearRegression()
reg.fit(X, y)
predictions = pd.DataFrame(reg.predict(X), index=df.index)
predictions.columns = ['pred']
print(predictions[-5:])

plt.scatter(df.index, df['VA'], color='blue')
plt.scatter(predictions.index, predictions['pred'], color='red')
plt.ylabel('basis value')
plt.show()

# Create training and test set

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.15, random_state=5)

reg = LinearRegression()
reg.fit(X_train, y_train)
y_pred = reg.predict(X_test)
print("Predictions: {}, Actual Values: {}".format(y_pred[:2], y_test[:2]))

# R-squareds and scores

r_squared = reg.score(X_test, y_test)
# rmse = mean_squared_error(y_test, y_pred, squared=False)

print("R^2: {}".format(r_squared))
# print("RMSE: {}".format(rmse))

# Cross Validation steps

kf = KFold(n_splits = 10, shuffle=True, random_state=5)
reg_cv = LinearRegression()
cv_scores = cross_val_score(reg_cv, X, y, cv=kf)
print(cv_scores)

print(f'Mean: ',   np.mean(cv_scores))
print(f'Standard Dev: ', np.std(cv_scores))
print(f'IQR CI: ', np.quantile(cv_scores, [0.25, .75]))

# Regularized Regressions

# Ridge regression
alphas = [0.1, 1, 10, 100, 1000, 10000]
ridge_scores = []
for alpha in alphas:
    ridge = Ridge(alpha=alpha)
    ridge.fit(X_train, y_train)
    score = ridge.score(X_test, y_test)
    ridge_scores.append(score)
    print(f'Completed {alpha} Ridge funcntion')

print(f'Ridge scores: ', ridge_scores)

# Lasso regression
lasso = Lasso(alpha=0.3)
lasso.fit(X, y)
lasso_coef = lasso.coef_
print(f'Lasso coeficients: ', lasso_coef)

df_columns = df.iloc[:, 5:].columns

plt.bar(df_columns, lasso_coef)
plt.xticks(rotation=45)
plt.show()