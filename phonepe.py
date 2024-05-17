import json
import requests
import pandas as pd
import streamlit as st
from PIL import Image
import mysql.connector
import plotly.express as px
from streamlit_option_menu import option_menu

#  Mysql Connections

mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database='phonepedata',
)

mycursor = mydb.cursor(buffered=True)

#aggre_transaction_df
mycursor.execute("SELECT * FROM aggregated_transaction")
mydb.commit()
table1= mycursor.fetchall()

Aggre_transaction= pd.DataFrame(table1, columns=("States", "Years", "Quarter", "Transaction_type",
                                               "Transaction_count", "Transaction_amount"))

#aggre_user_df
mycursor.execute("SELECT * FROM aggregated_user")
mydb.commit()
table2= mycursor.fetchall()

Aggre_user= pd.DataFrame(table2, columns=("States", "Years", "Quarter", "Brands",
                                               "Transaction_count", "Percentage"))

#map_transction
mycursor.execute("SELECT * FROM map_transaction")
mydb.commit()
table3= mycursor.fetchall()

map_transaction= pd.DataFrame(table3, columns=("States", "Years", "Quarter", "District",
                                               "Transaction_count", "Transaction_amount"))

#map_user
mycursor.execute("SELECT * FROM map_user")
mydb.commit()
table4= mycursor.fetchall()

map_user= pd.DataFrame(table4, columns=("States", "Years", "Quarter", "District",
                                               "RegisteredUser", "AppOpens"))

#top_transaction
mycursor.execute("SELECT * FROM top_transaction")
mydb.commit()
table5= mycursor.fetchall()

top_transaction= pd.DataFrame(table5, columns=("States", "Years", "Quarter", "Pincodes",
                                               "Transaction_count", "Transaction_amount"))

#top_user
mycursor.execute("SELECT * FROM top_user")
mydb.commit()
table6= mycursor.fetchall()

top_user= pd.DataFrame(table6, columns=("States", "Years", "Quarter", "Pincodes",
                                               "RegisteredUsers"))


#------------------------------------------------------------------------------------------------------------------------------------------
#Top charts Queries part
# Question 1&2&3...
def top_chart_transaction_amount(table_name):
    query1= f'''SELECT states, SUM(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount DESC
                LIMIT 10;'''
    mycursor.execute(query1)
    table_1= mycursor.fetchall()
    mydb.commit()
    df_1= pd.DataFrame(table_1, columns=("states", "transaction_amount"))
    col1,col2= st.columns(2)
    with col1:
        fig_amount= px.bar(df_1, x="states", y="transaction_amount", title="TOP 10 OF TRANSACTION AMOUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        st.plotly_chart(fig_amount)

    #plot_2
    query2= f'''SELECT states, SUM(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount
                LIMIT 10;'''
    mycursor.execute(query2)
    table_2= mycursor.fetchall()
    mydb.commit()
    df_2= pd.DataFrame(table_2, columns=("states", "transaction_amount"))
    with col2:
        fig_amount_2= px.bar(df_2, x="states", y="transaction_amount", title="LAST 10 OF TRANSACTION AMOUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        st.plotly_chart(fig_amount_2)

    #plot_3
    query3= f'''SELECT states, AVG(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount;'''
    mycursor.execute(query3)
    table_3= mycursor.fetchall()
    mydb.commit()
    df_3= pd.DataFrame(table_3, columns=("states", "transaction_amount"))
    fig_amount_3= px.bar(df_3, y="states", x="transaction_amount", title="AVERAGE OF TRANSACTION AMOUNT", hover_name= "states", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1300)
    st.plotly_chart(fig_amount_3)
# ---------------------------------------------------------------------------------------------------------------------------------------------------

def top_chart_transaction_count(table_name):
    query1= f'''SELECT states, SUM(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count DESC
                LIMIT 10;'''
    mycursor.execute(query1)
    table_1= mycursor.fetchall()
    mydb.commit()
    df_1= pd.DataFrame(table_1, columns=("states", "transaction_count"))
    col1,col2= st.columns(2)
    with col1:
        fig_amount= px.bar(df_1, x="states", y="transaction_count", title="TOP 10 OF TRANSACTION COUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        st.plotly_chart(fig_amount)

    #plot_2
    query2= f'''SELECT states, SUM(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count
                LIMIT 10;'''
    mycursor.execute(query2)
    table_2= mycursor.fetchall()
    mydb.commit()
    df_2= pd.DataFrame(table_2, columns=("states", "transaction_count"))
    with col2:
        fig_amount_2= px.bar(df_2, x="states", y="transaction_count", title="LAST 10 OF TRANSACTION COUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        st.plotly_chart(fig_amount_2)


    #plot_3
    query3= f'''SELECT states, AVG(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count;'''
    mycursor.execute(query3)
    table_3= mycursor.fetchall()
    mydb.commit()
    df_3= pd.DataFrame(table_3, columns=("states", "transaction_count"))
    fig_amount_3= px.bar(df_3, y="states", x="transaction_count", title="AVERAGE OF TRANSACTION COUNT", hover_name= "states", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1300)
    st.plotly_chart(fig_amount_3)
# --------------------------------------------------------------------------------------------------------------------------------------------------

# Question:5. RegisteredUsers of Map User
def top_chart_registered_user(table_name, state):
    #plot_1
    query1= f'''SELECT Districts, SUM(Registeredusers) AS registereduser
                FROM {table_name}
                WHERE states= '{state}'   -- Quote the state variable
                GROUP BY districts
                ORDER BY registereduser DESC
                LIMIT 10;'''


    mycursor.execute(query1)
    table_1= mycursor.fetchall()
    mydb.commit()
    df_1= pd.DataFrame(table_1, columns=("districts", "registereduser"))
    col1,col2= st.columns(2)
    with col1:
        fig_amount= px.bar(df_1, x="districts", y="registereduser", title="TOP 10 OF REGISTERED USER", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        st.plotly_chart(fig_amount)

    #plot_2
    query2= f'''SELECT Districts, SUM(Registeredusers) AS registereduser
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY registereduser
                LIMIT 10;'''

    mycursor.execute(query2)
    table_2= mycursor.fetchall()
    mydb.commit()
    df_2= pd.DataFrame(table_2, columns=("districts", "registereduser"))
    with col2:
        fig_amount_2= px.bar(df_2, x="districts", y="registereduser", title="LAST 10 REGISTERED USER", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        st.plotly_chart(fig_amount_2)
    #plot_3
    query3= f'''SELECT Districts, AVG(Registeredusers) AS registereduser
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY registereduser;'''

    mycursor.execute(query3)
    table_3= mycursor.fetchall()
    mydb.commit()

    df_3= pd.DataFrame(table_3, columns=("districts", "registereduser"))

    fig_amount_3= px.bar(df_3, y="districts", x="registereduser", title="AVERAGE OF REGISTERED USER", hover_name= "districts", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 600,width= 1200)
    st.plotly_chart(fig_amount_3)
# ---------------------------------------------------------------------------------------------------------------------------------------------------

# Question: 6. AppOpens of Map User
def top_chart_appopens(table_name, state):
    #plot_1
    query1= f'''SELECT Districts, SUM(AppOpens) AS appopens
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY appopens DESC
                LIMIT 10;'''

    mycursor.execute(query1)
    table_1= mycursor.fetchall()
    mydb.commit()

    df_1= pd.DataFrame(table_1, columns=("districts", "appopens"))


    col1,col2= st.columns(2)
    with col1:

        fig_amount= px.bar(df_1, x="districts", y="appopens", title="TOP 10 OF APPOPENS", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        st.plotly_chart(fig_amount)

    #plot_2
    query2= f'''SELECT Districts, SUM(AppOpens) AS appopens
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY appopens
                LIMIT 10;'''

    mycursor.execute(query2)
    table_2= mycursor.fetchall()
    mydb.commit()

    df_2= pd.DataFrame(table_2, columns=("districts", "appopens"))

    with col2:

        fig_amount_2= px.bar(df_2, x="districts", y="appopens", title="LAST 10 APPOPENS", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        st.plotly_chart(fig_amount_2)

    #plot_3
    query3= f'''SELECT Districts, AVG(AppOpens) AS appopens
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY appopens;'''

    mycursor.execute(query3)
    table_3= mycursor.fetchall()
    mydb.commit()

    df_3= pd.DataFrame(table_3, columns=("districts", "appopens"))

    fig_amount_3= px.bar(df_3, y="districts", x="appopens", title="AVERAGE OF APPOPENS", hover_name= "districts", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1000)
    st.plotly_chart(fig_amount_3)
#----------------------------------------------------------------------------------------------------------------------------------------------

#Question: 7. RegisteredUsers of Top User

def top_chart_registered_users(table_name):
    #plot_1
    query1= f'''SELECT states, SUM(RegisteredUsers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers DESC
                LIMIT 10;'''

    mycursor.execute(query1)
    table_1= mycursor.fetchall()
    mydb.commit()

    df_1= pd.DataFrame(table_1, columns=("states", "registeredusers"))
    
    col1,col2= st.columns(2)
    with col1:

        fig_amount= px.bar(df_1, x="states", y="registeredusers", title="TOP 10 OF REGISTERED USERS", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        st.plotly_chart(fig_amount)

    #plot_2
    query2= f'''SELECT states, SUM(RegisteredUsers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers
                LIMIT 10;'''

    mycursor.execute(query2)
    table_2= mycursor.fetchall()
    mydb.commit()

    df_2= pd.DataFrame(table_2, columns=("states", "registeredusers"))

    with col2:

        fig_amount_2= px.bar(df_2, x="states", y="registeredusers", title="LAST 10 REGISTERED USERS", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        st.plotly_chart(fig_amount_2)

    #plot_3
    query3= f'''SELECT states, AVG(RegisteredUsers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers;'''

    mycursor.execute(query3)
    table_3= mycursor.fetchall()
    mydb.commit()

    df_3= pd.DataFrame(table_3, columns=("states", "registeredusers"))

    fig_amount_3= px.bar(df_3, y="states", x="registeredusers", title="AVERAGE OF REGISTERED USERS", hover_name= "states", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1200)
    st.plotly_chart(fig_amount_3)

#-----------------------------------------------------------------------------------------------------------------------------------------------------


#Question: 8. Brands of Aggregrated User

def top_chart_aggregrated_users(table_name):
    #plot_1
    query1= f'''SELECT Brands, SUM(Transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY Brands
                ORDER BY transaction_count DESC
                LIMIT 10;'''

    mycursor.execute(query1)
    table_1= mycursor.fetchall()
    mydb.commit()

    df_1= pd.DataFrame(table_1, columns=("Brands", "transaction_count"))
    
    col1,col2= st.columns(2)
    with col1:

        fig_amount= px.bar(df_1, x="Brands", y="transaction_count", title="TOP 10 OF BRAND USERS", hover_name= "Brands",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        st.plotly_chart(fig_amount)

    #plot_2
    query2= f'''SELECT Brands, SUM(Transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY Brands
                ORDER BY transaction_count
                LIMIT 10;'''

    mycursor.execute(query2)
    table_2= mycursor.fetchall()
    mydb.commit()

    df_2= pd.DataFrame(table_2, columns=("Brands", "transaction_count"))

    with col2:

        fig_amount_2= px.bar(df_2, x="Brands", y="transaction_count", title="LAST 10 BRAND USERS", hover_name= "Brands",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height= 650,width= 600)
        st.plotly_chart(fig_amount_2)

    #plot_3
    query3= f'''SELECT Brands, AVG(Transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY Brands
                ORDER BY transaction_count;'''

    mycursor.execute(query3)
    table_3= mycursor.fetchall()
    mydb.commit()

    df_3= pd.DataFrame(table_3, columns=("Brands", "transaction_count"))

    fig_amount_3= px.bar(df_3, y="Brands", x="transaction_count", title="AVERAGE OF BRAND USERS", hover_name= "Brands", orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height= 800,width= 1200)
    st.plotly_chart(fig_amount_3)

#-----------------------------------------------------------------------------------------------------------------------------------------------

# Question: 6. AppOpens of Map User
def sum_transaction_count_by_type(table_name, state):
    query1 = f'''SELECT Transaction_type, SUM(Transaction_count) AS total_transaction_count
                FROM {table_name}
                WHERE States = '{state}'
                GROUP BY Transaction_type;'''

    mycursor.execute(query1)
    table_1= mycursor.fetchall()
    mydb.commit()

    df_1= pd.DataFrame(table_1, columns=("Transaction_type", "transaction_count"))


    col1,col2= st.columns(2)
    with col1:

        fig_amount= px.bar(df_1, x="Transaction_type", y="transaction_count", title="TOP 10 OF APPOPENS", hover_name= "Transaction_type",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height= 650,width= 600)
        st.plotly_chart(fig_amount)



# ---------------------------------------------------------------------------------------------------------------------------------------------------
# Aggregated Transaction Year..
def Transaction_amount_count_Y(df, year):
    
    tacy= df[df["Years"] == year]
    tacy.reset_index(drop= True, inplace= True)

    tacyg=tacy.groupby("States")[["Transaction_count", "Transaction_amount"]].sum()
    tacyg.reset_index(inplace= True)



    col1,col2 = st.columns(2)
    with col1:
        fig_amount=px.bar(tacy,x="States",y="Transaction_amount",title=f"{year}  TRANSACTION AMOUNT",
                          color_discrete_sequence=px.colors.sequential.Aggrnyl,height=650,width=600)
        st.plotly_chart(fig_amount)
    with col2:
        fig_count=px.bar(tacy,x="States",y="Transaction_count",title=f"{year}  TRANSACTION COUNT",
                         color_discrete_sequence=px.colors.sequential.Jet_r,height=650,width=600)
        st.plotly_chart(fig_count)
    url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    try:
        response= requests.get(url)
        data= json.loads(response.content)
        states_name= []
        for feature in data["features"]:
            states_name.append(feature["properties"]["ST_NM"])
        states_name.sort()

        col1,col2 = st.columns(2)
        with col1:
            fig_india_1= px.choropleth(tacyg, geojson= data, locations= "States", featureidkey= "properties.ST_NM",
                                            color= "Transaction_amount", color_continuous_scale= "temps",
                                            range_color= (tacyg["Transaction_amount"].min(), tacyg["Transaction_amount"].max()),
                                            hover_name= "States", title= f"{year} TRANSACTION AMOUNT", fitbounds= "locations",
                                            height= 650,width= 600)
            fig_india_1.update_geos(visible= False)
            st.plotly_chart(fig_india_1)
    except:
        print("error")
    with col2:
      fig_india_2= px.choropleth(tacyg, geojson= data, locations= "States", featureidkey= "properties.ST_NM",
                                    color= "Transaction_count", color_continuous_scale= "temps",
                                    range_color= (tacyg["Transaction_count"].min(), tacyg["Transaction_count"].max()),
                                    hover_name= "States", title= f"{year} TRANSACTION COUNT", fitbounds= "locations",
                                    height= 650,width= 600)
      fig_india_2.update_geos(visible= False)
      st.plotly_chart(fig_india_2)

      return tacy

def Transaction_amount_count_Y_Q(df,quarter):
    tacy= df[df["Quarter"] == quarter]
    tacy.reset_index(drop= True, inplace= True)

    tacyg=tacy.groupby("States")[["Transaction_count", "Transaction_amount"]].sum()
    tacyg.reset_index(inplace= True)
    col1,col2 = st.columns(2)
    with col1:
      fig_amount=px.bar(tacy,x="States",y="Transaction_amount",title= f"{tacy['Years'].min()} YEAR {quarter} QUARTER TRANSACTION AMOUNT",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl,height= 650,width= 600)
      st.plotly_chart(fig_amount)
    with col2:
      fig_count=px.bar(tacy,x="States",y="Transaction_count",title=f"{tacy['Years'].min()} YEAR {quarter} QUARTER TRANSACTION COUNT",
                        color_discrete_sequence=px.colors.sequential.Jet_r,height= 650,width= 600)
      st.plotly_chart(fig_count)
    
    url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response= requests.get(url)
    data= json.loads(response.content)
    states_name= []
    for feature in data["features"]:
        states_name.append(feature["properties"]["ST_NM"])
    states_name.sort()
    col1,col2 = st.columns(2)
    with col1:
      fig_india_1= px.choropleth(tacyg, geojson= data, locations= "States", featureidkey= "properties.ST_NM",
                                    color= "Transaction_amount", color_continuous_scale= "temps",
                                    range_color= (tacyg["Transaction_amount"].min(), tacyg["Transaction_amount"].max()),
                                    hover_name= "States", title= f"{tacy['Years'].min()} YEAR {quarter} QUARTER TRANSACTION AMOUNT", fitbounds= "locations",
                                    height= 650,width= 600)
      fig_india_1.update_geos(visible= False)
      st.plotly_chart(fig_india_1)
    with col2:
      fig_india_2= px.choropleth(tacyg, geojson= data, locations= "States", featureidkey= "properties.ST_NM",
                                    color= "Transaction_count", color_continuous_scale= "temps",
                                    range_color= (tacyg["Transaction_count"].min(), tacyg["Transaction_count"].max()),
                                    hover_name= "States", title= f"{tacy['Years'].min()} YEAR {quarter} QUARTER TRANSACTION COUNT", fitbounds= "locations",
                                    height= 650,width= 600)
      fig_india_2.update_geos(visible= False)
      st.plotly_chart(fig_india_2) 
      return tacy

def Aggre_Tran_Transaction_type(df, state):

    tacy_P=df[ df["States"]==state]
    tacy_P.reset_index(drop=True,inplace=True)

    tacyg=tacy_P.groupby("Transaction_type")[["Transaction_count", "Transaction_amount"]].sum()
    tacyg.reset_index(inplace= True)
    col1,col2 = st.columns(2)
    with col1:
      fig_pie_1= px.pie(data_frame= tacyg, names= "Transaction_type", values= "Transaction_amount",
                              width= 600, title= f"{state.upper()}  TRANSACTION AMOUNT", hole= 0.5)
      st.plotly_chart(fig_pie_1)
    with col2:
      fig_pie_2= px.pie(data_frame= tacyg, names= "Transaction_type", values= "Transaction_count",
                              width= 600, title= f"{state.upper()}  TRANSACTION COUNT", hole= 0.5)
      st.plotly_chart(fig_pie_2)

def Aggre_user_plot_1(df, year):
    aguy= df[df["Years"]== year]
    aguy.reset_index(drop= True, inplace= True)

    aguyg= pd.DataFrame(aguy.groupby("Brands")["Transaction_count"].sum())
    aguyg.reset_index(inplace= True)

    fig_bar_1= px.bar(aguyg, x= "Brands", y= "Transaction_count", title= f"{year} BRANDS AND TRANSACTION COUNT",
                    width= 1200, color_discrete_sequence= px.colors.sequential.haline_r, hover_name= "Brands")
    st.plotly_chart(fig_bar_1)

    return aguy

def Aggre_user_plot_2(df, quarter):
    aguyq= df[df["Quarter"]== quarter]
    aguyq.reset_index(drop= True, inplace= True)

    aguyqg= pd.DataFrame(aguyq.groupby("Brands")["Transaction_count"].sum())
    aguyqg.reset_index(inplace= True)

    fig_bar_1= px.bar(aguyqg, x= "Brands", y= "Transaction_count", title=  f"{quarter} QUARTER, BRANDS AND TRANSACTION COUNT",
                    width= 1200, color_discrete_sequence= px.colors.sequential.Aggrnyl, hover_name="Brands")
    st.plotly_chart(fig_bar_1)

    return aguyq

#Aggre_user_alalysis_3
def Aggre_user_plot_3(df, state):
    auyqs= df[df["States"] == state]
    auyqs.reset_index(drop= True, inplace= True)

    fig_line_1= px.line(auyqs, x= "Brands", y= "Transaction_count", hover_data= "Percentage",
                        title= f"{state.upper()} BRANDS, TRANSACTION COUNT, PERCENTAGE",width= 1200, markers= True)
    st.plotly_chart(fig_line_1)

# Map Transaction Districts
def Map_Tran_District(df, state):
    tacy_P=df[ df["States"]==state]
    tacy_P.reset_index(drop=True,inplace=True)

    tacyg=tacy_P.groupby("District")[["Transaction_count", "Transaction_amount"]].sum()
    tacyg.reset_index(inplace= True)
    col1,col2 = st.columns(2)
    with col1:
      fig_bar_1= px.bar(tacyg, x= "Transaction_amount",y= "District",orientation="h",title=f"{state.upper()} DISTRICT AND TRANSACTION AMOUNT",
                          height=600,color_discrete_sequence=px.colors.sequential.Mint_r)
      st.plotly_chart(fig_bar_1)
    with col2:
      fig_bar_2= px.bar(tacyg, x= "Transaction_count",y= "District",orientation="h",title=f"{state.upper()} DISTRICT AND TRANSACTION COUNT",
                          height=600,color_discrete_sequence=px.colors.sequential.speed)
      st.plotly_chart(fig_bar_2)

# Map user Year Plot 1
def map_user_plot_1(df, year):
    muy= df[df["Years"]== year]
    muy.reset_index(drop= True, inplace= True)

    muyg= pd.DataFrame(muy.groupby("States")[["RegisteredUser","AppOpens"]].sum())
    muyg.reset_index(inplace= True)
    fig_line_1= px.line(muyg, x= "States", y= ["RegisteredUser","AppOpens"],
                        title= f"{year} REGISTERESUSERS & APPOPENS",width= 1300,height=700, markers= True)
    st.plotly_chart(fig_line_1)
    return muy

# Map user Quarter Plot 2
def map_user_plot_2(df, quarter):
    muyq= df[df["Quarter"]== quarter]
    muyq.reset_index(drop= True, inplace= True)

    muyqg= muyq.groupby("States")[["RegisteredUser", "AppOpens"]].sum()
    muyqg.reset_index(inplace= True)

    fig_line_1= px.line(muyqg, x= "States", y= ["RegisteredUser", "AppOpens"],
                        title= f"{df['Years'].min()} YEARS {quarter} QUARTER REGISTEREDUSER & APPOPENS",width= 1300, height= 700, markers= True,
                        color_discrete_sequence= px.colors.sequential.Hot_r)
    st.plotly_chart(fig_line_1)
    return muyq

#Map user states plot_3
def map_user_plot_3(df, states):
    muyqs= df[df["States"]== states]
    muyqs.reset_index(drop= True, inplace= True)

    col1,col2= st.columns(2)
    with col1:
        fig_map_user_bar1= px.bar(muyqs, x= "RegisteredUser", y= "District", orientation= "h",title= f"{states.upper()} REGISTERED USER",
                                  height= 600, color_discrete_sequence= px.colors.sequential.haline_r)
        st.plotly_chart(fig_map_user_bar1)

    with col2:

        fig_map_user_bar2= px.bar(muyqs, x= "AppOpens", y= "District", orientation= "h",title= f"{states.upper()} APPOPENS",
                                    height= 600, color_discrete_sequence= px.colors.sequential.Sunset_r)
        st.plotly_chart(fig_map_user_bar2)

# Top Transaction year wise quarter plot1..
def Top_Tran_Q_plot_1(df,state):
    tty= df[df["States"]== state]
    tty.reset_index(drop= True, inplace= True)

    ttyg= tty.groupby("Pincodes")[["Transaction_count", "Transaction_amount"]].sum()
    ttyg.reset_index(inplace= True)
    col1,col2= st.columns(2)
    with col1:
        fig_top_tran_bar_1= px.bar(tty, x= "Quarter", y= "Transaction_amount",hover_data="Pincodes",title="TRANSACTION AMOUNT",
                                        height= 600,width=600, color_discrete_sequence= px.colors.sequential.Tealgrn_r)
        st.plotly_chart(fig_top_tran_bar_1)
    with col2:
        fig_top_tran_bar_2= px.bar(tty, x= "Quarter", y= "Transaction_count",hover_data="Pincodes",title="TRANSACTION COUNT",
                                        height= 600,width=600, color_discrete_sequence= px.colors.sequential.Viridis_r)
        st.plotly_chart(fig_top_tran_bar_2)

# Top User Year plot1
def top_user_plot_1(df,year):
    tuy= df[df["Years"] == year]
    tuy.reset_index(drop= True, inplace= True)

    tuyg= pd.DataFrame(tuy.groupby(["States","Quarter"])["RegisteredUsers"].sum())
    tuyg.reset_index(inplace= True)

    fig_top_plot_1= px.bar(tuyg, x= "States", y= "RegisteredUsers", barmode= "group", color= "Quarter",
                            width=1300, height= 700, color_discrete_sequence= px.colors.sequential.Burgyl)
    st.plotly_chart(fig_top_plot_1)

    return tuy

def top_user_plot_2(df,state):
    tuys= df[df["States"] == state]
    tuys.reset_index(drop= True, inplace= True)

    tuysg= pd.DataFrame(tuys.groupby("Quarter")["RegisteredUsers"].sum())
    tuysg.reset_index(inplace= True)

    fig_top_plot_2= px.bar(tuys, x= "Quarter", y= "RegisteredUsers",barmode= "group",
                           width=1000, height= 800,color= "RegisteredUsers",hover_data="Pincodes",
                            color_continuous_scale= px.colors.sequential.Magenta)
    st.plotly_chart(fig_top_plot_2)


# streamlit part 

st.set_page_config(layout="wide")
st.title("PHONEPE DATA VISUALIZATION AND EXPLORATION")
st.sidebar.header(":wave: :violet[**Hello! Welcome to the dashboard**]")

with st.sidebar:
      select= option_menu("Main Menu",["HOME","DATA EXPLORATION","TOP CHARTS","ABOUT"],
      icons=["house","graph-up-arrow","bar-chart-line", "exclamation-circle"],
      menu_icon= "menu-button-wide",
      default_index=0,
      styles={"nav-link": {"font-size": "20px", "text-align": "left", "margin": "-2px", "--hover-color": "#6F36AD"},
                        "nav-link-selected": {"background-color": "#6F36AD"}})

if select == "HOME":
    
    col1,col2= st.columns(2)

    with col1:
        st.header("PHONEPE")
        st.subheader("INDIA'S BEST TRANSACTION APP")
        st.markdown("PhonePe  is an Indian digital payments and financial technology company")
        st.write("****FEATURES****")
        st.write("****Credit & Debit card linking****")
        st.write("****Bank Balance check****")
        st.write("****Money Storage****")
        st.write("****PIN Authorization****")
        st.download_button("DOWNLOAD THE APP NOW", "https://play.google.com/store/apps/details?id=com.phonepe.app&hl=en_IN&shortlink=2kk1w03o&c=consumer_app_icon&pid=PPWeb_app_download_page&af_xp=custom&source_caller=ui")

    col3,col4= st.columns(2)
    
    with col2:     
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.video(r"C:\Users\ss996\OneDrive\Desktop\Projects\P2-PP\PhonePe_-_Introduction(720p).mp4")

    col3,col4= st.columns(2)
    with col3:
        st.video(r"C:\Users\ss996\OneDrive\Desktop\Projects\P2-PP\PhonePe_Motion_Graphics_in_After_Effects___Naveen_Shiva_Sai___Student_Works(1080p).mp4")

    with col4:
        st.write("****Easy Transactions****")
        st.write("****One App For All Your Payments****")
        st.write("****Your Bank Account Is All You Need****")
        st.write("****Multiple Payment Modes****")
        st.write("****PhonePe Merchants****")
        st.write("****Multiple Ways To Pay****")
        st.write("****1.Direct Transfer & More****")
        st.write("****2.QR Code****")
        st.write("****Earn Great Rewards****")

    col5,col6= st.columns(2)

    with col5:
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.write("****No Wallet Top-Up Required****")
        st.write("****Pay Directly From Any Bank To Any Bank A/C****")
        st.write("****Instantly & Free****")

        with col6:
            st.video(r"C:\Users\ss996\OneDrive\Desktop\Projects\P2-PP\PhonePe_Motion_Graphics(1080p).mp4")


elif select == "DATA EXPLORATION":
      tab1,tab2,tab3=st.tabs(["Aggregated Analysis","Map Analysis","Top Analysis"])
      with tab1:
            method1 = st.radio("Select The Method:",["Aggregated Transations","Aggregated Users"])
            if method1 == "Aggregated Transations":
                  col1,col2 = st.columns(2)
                  with col1:
                        years=st.slider("Select the Year:",Aggre_transaction["Years"].min(),Aggre_transaction["Years"].max(),Aggre_transaction["Years"].min())
                  tacy=Transaction_amount_count_Y(Aggre_transaction,years)

                  col1,col2 = st.columns(2)
                  with col1:
                       quarters=st.slider("Select the Quarter:",tacy["Quarter"].min(),tacy["Quarter"].max(),tacy["Quarter"].min())
                  tac_Y=Transaction_amount_count_Y_Q(tacy,quarters)

                  col1,col2 = st.columns(2)
                  with col1:
                      states=st.selectbox("Select the State",tac_Y["States"].unique())
                  Aggre_Tran_Transaction_type(tac_Y, states)



            elif method1 == "Aggregated Users":
                  col1,col2 = st.columns(2)
                  with col1:
                       years=st.slider("Select the Year:",Aggre_user["Years"].min(),Aggre_user["Years"].max(),Aggre_user["Years"].min())
                  Aggre_user_Y=Aggre_user_plot_1(Aggre_user,years)

                  col1,col2 = st.columns(2)
                  with col1:
                       quarters=st.slider("Select the Quarter:",Aggre_user_Y["Quarter"].min(),Aggre_user_Y["Quarter"].max(),Aggre_user_Y["Quarter"].min())
                  Aggre_user_Y_Q=Aggre_user_plot_2(Aggre_user_Y,quarters)

                  col1,col2 = st.columns(2)
                  with col1:
                      states=st.selectbox("Select the State",Aggre_user_Y_Q["States"].unique())
                  Aggre_user_plot_3(Aggre_user_Y_Q, states)

      with tab2:
            method2 = st.radio("Select The Method:",["Map Transations","Map Users"])
            if method2 == "Map Transations":
                  col1,col2 = st.columns(2)
                  with col1:
                        years=st.slider("Select The Year:",map_transaction["Years"].min(),map_transaction["Years"].max(),map_transaction["Years"].min())
                  map_tran_tac_Y=Transaction_amount_count_Y(map_transaction,years) 
                  col1,col2 = st.columns(2)
                  with col1:
                      states=st.selectbox("Select he State Map_Trans",map_tran_tac_Y["States"].unique())
                  Map_Tran_District(map_tran_tac_Y, states)
                  col1,col2 = st.columns(2)
                  with col1:
                       quarters=st.slider("Select the Quarters:",map_tran_tac_Y["Quarter"].min(),map_tran_tac_Y["Quarter"].max(),map_tran_tac_Y["Quarter"].min())
                  map_tran_tac_Y_Q=Transaction_amount_count_Y_Q(map_tran_tac_Y,quarters)
            elif method2 == "Map Users":
                  col1,col2 = st.columns(2)
                  with col1:
                        years=st.slider("Select The Year:",map_user["Years"].min(),map_user["Years"].max(),map_user["Years"].min())
                  Map_user_Y=map_user_plot_1(map_user,years) 
                  col1,col2 = st.columns(2)
                  with col1:
                       quarters=st.slider("Select the Quarters Map User:",Map_user_Y["Quarter"].min(),Map_user_Y["Quarter"].max(),Map_user_Y["Quarter"].min())
                  map_user_Y_Q=map_user_plot_2(Map_user_Y,quarters)
                  col1,col2 = st.columns(2)
                  with col1:
                       states=st.selectbox("Select the Quarters Map User:",map_user_Y_Q["States"].unique())
                  map_user_plot_3(map_user_Y_Q, states)

      with tab3:
            method3 = st.radio("Select The Method:",["Top Transations","Top Users"])
            if method3 == "Top Transations":
                col1,col2 = st.columns(2)
                with col1:
                        years=st.slider("Select The Top Year:",top_transaction["Years"].min(),top_transaction["Years"].max(),top_transaction["Years"].min())
                top_tran_tac_Y=Transaction_amount_count_Y(top_transaction,years) 
                col1,col2 = st.columns(2)
                with col1:
                       states=st.selectbox("Select the State:",top_tran_tac_Y["States"].unique())
                Top_Tran_Q_plot_1(top_tran_tac_Y,states)
                col1,col2 = st.columns(2)
                with col1:
                    quarters=st.slider("Select the Quarters Top transaction:",top_tran_tac_Y["Quarter"].min(),top_tran_tac_Y["Quarter"].max(),top_tran_tac_Y["Quarter"].min())
                top_tran_Y_Q=Transaction_amount_count_Y_Q(top_tran_tac_Y,quarters)

            elif method3 == "Top Users":
                col1,col2 = st.columns(2)
                with col1:
                        years=st.slider("Select The Top Year:",top_user["Years"].min(),top_user["Years"].max(),top_user["Years"].min())
                Top_user_Y= top_user_plot_1(top_user,years) 
                col1,col2 = st.columns(2)
                with col1:
                       states=st.selectbox("Select the State:",Top_user_Y["States"].unique())
                top_user_plot_2(Top_user_Y,states)

elif select == "TOP CHARTS":
      question = st.selectbox("Select the Questions :",["1. Transaction Amount of Aggregated Transaction",
                                                        "2.Transaction Count of Aggregated Transaction",
                                                        "3. Transaction Amount of Map Transaction",
                                                        "4. Transaction Count of Map Transaction",
                                                        "5. Transaction Amount  of Top Transaction",
                                                        "6.Transaction Count of Top Transaction",
                                                        "7. Transaction Count of Aggregated User",
                                                        "8. RegisteredUsers of Map User",
                                                        "9. AppOpens of Map User",
                                                        "10. RegisteredUsers of Top User",
                                                        "11. Brands of Aggregated User"])
      if question=="1. Transaction Amount of Aggregated Transaction":
          st.subheader("TRANSACTION AMOUNT")
          top_chart_transaction_amount("aggregated_transaction")

      elif question=="2.Transaction Count of Aggregated Transaction":
          st.subheader("TRANSACTION COUNT")
          top_chart_transaction_count("aggregated_transaction")

      elif question=="3. Transaction Amount of Map Transaction":
          st.subheader("TRANSACTION AMOUNT")
          top_chart_transaction_amount("map_transaction")

      elif question=="4. Transaction Count of Map Transaction":
          st.subheader("TRANSACTION COUNT")
          top_chart_transaction_count("map_transaction")

      elif question=="5. Transaction Amount  of Top Transaction":
          st.subheader("TRANSACTION AMOUNT")
          top_chart_transaction_amount("top_transaction")

      elif question=="6.Transaction Count of Top Transaction":
          st.subheader("TRANSACTION COUNT")
          top_chart_transaction_count("top_transaction")
      
      elif question=="7. Transaction Count of Aggregated User":
          st.subheader("TRANSACTION COUNT")
          top_chart_transaction_count("aggregated_user")

      elif question=="8. RegisteredUsers of Map User":
          states=st.selectbox("Select the State",map_user['States'].unique())
          st.subheader("REGISTERED USERS")
          top_chart_registered_user("map_user", states)

      elif question=="9. AppOpens of Map User":
          states=st.selectbox("Select the State",map_user['States'].unique())
          st.subheader("APPOPENS")
          top_chart_appopens("map_user", states)

      elif question=="10. RegisteredUsers of Top User":
          st.subheader("REGISTERED USERS")
          top_chart_registered_users("top_user")

      elif question=="11. Brands of Aggregated User":
          st.subheader("AGGREGRATED USERS")
          top_chart_aggregrated_users("aggregated_user")       
      
