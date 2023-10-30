import pyodbc 
import pandas as pd

class Delete_SO:
    global SO_ID 
    SO_ID = '3760130941'
    conn = ("""driver={SQL Server};server={{Database port}};database={{Database Name}};
        trusted_connection=no;UID={{Database User Name}};PWD={{Database Password}};IntegratedSecurity = true;""")
    conx = pyodbc.connect(conn)
    cur = conx.cursor()

    def Header_query(self):             # 1 = ARTSANA_OUTBOUND_DELIVERY_HEADER

        sql_query1 = pd.read_sql_query("SELECT * FROM ARTSANA_OUTBOUND_DELIVERY_HEADER WHERE CONSEGNA = '"+SO_ID+"'",self.conx)
        df = pd.DataFrame(sql_query1)
        df.to_csv(r'C:\Users\Emiza\Desktop\BACKUP_ARTSANA_SO\Backup_ARTSANA_OUTBOUND_DELIVERY_HEADER.csv' , index= False)
        print("ARTSANA_OUTBOUND_DELIVERY_HEADER export complete")   

        Delete_query1 = "DELETE FROM ARTSANA_OUTBOUND_DELIVERY_HEADER WHERE CONSEGNA = '"+SO_ID+"'"
        exe1 = self.cur.execute(Delete_query1)
        self.cur.commit()
        print("ARTSANA_OUTBOUND_DELIVERY_HEADER Deleted Successfully",SO_ID)


    def Delivery_Item(self):            # 2 = ARTSANA_OUTBOUND_DELIVERY_ITEM

        sql_query2 = pd.read_sql_query("SELECT * FROM ARTSANA_OUTBOUND_DELIVERY_ITEM WHERE DELIVERY = '"+SO_ID+"'",self.conx)
        df = pd.DataFrame(sql_query2)
        df.to_csv(r'C:\Users\Emiza\Desktop\BACKUP_ARTSANA_SO\Backup_ARTSANA_OUTBOUND_DELIVERY_ITEM.csv' , index= False)
        print("ARTSANA_OUTBOUND_DELIVERY_ITEM export complete")   

        Delete_query2 = "DELETE FROM ARTSANA_OUTBOUND_DELIVERY_ITEM WHERE DELIVERY = '"+SO_ID+"'"
        exe2 = self.cur.execute(Delete_query2)
        self.cur.commit()
        print("ARTSANA_OUTBOUND_DELIVERY_ITEM Deleted Successfully",SO_ID)


    def Delhivery_Qty(self):            # 3 = ARTSANA_OUTBOUND_DELIVERY_QUANTITY DETAIL

        sql_query3 = pd.read_sql_query("SELECT * FROM [ARTSANA_OUTBOUND_DELIVERY_QUANTITY DETAIL] WHERE DELIVERY = '"+SO_ID+"'",self.conx)
        df = pd.DataFrame(sql_query3)
        df.to_csv(r'C:\Users\Emiza\Desktop\BACKUP_ARTSANA_SO\Backup_ARTSANA_OUTBOUND_DELIVERY_QUANTITY DETAIL.csv' , index= False)
        print("ARTSANA_OUTBOUND_DELIVERY_QUANTITY DETAIL export complete")   

        Delete_query3 = "DELETE FROM [ARTSANA_OUTBOUND_DELIVERY_QUANTITY DETAIL] WHERE DELIVERY = '"+SO_ID+"'"
        exe3 = self.cur.execute(Delete_query3)
        self.cur.commit()
        print("ARTSANA_OUTBOUND_DELIVERY_QUANTITY DETAIL Deleted Successfully",SO_ID)


    def Delhivery_SO(self):             # 4 = ARTSANA_OUTBOUND_DELIVERY_SALES ORDER_NOTE

        sql_query4 = pd.read_sql_query("SELECT * FROM [ARTSANA_OUTBOUND_DELIVERY_SALES ORDER NOTE] WHERE DELIVERY = '"+SO_ID+"'",self.conx)
        df = pd.DataFrame(sql_query4)
        df.to_csv(r'C:\Users\Emiza\Desktop\BACKUP_ARTSANA_SO\Backup_ARTSANA_OUTBOUND_DELIVERY_SALES ORDER NOTE.csv' , index= False)
        print("ARTSANA_OUTBOUND_DELIVERY_SALES ORDER NOTE export complete")   

        Delete_query4 = "DELETE FROM [ARTSANA_OUTBOUND_DELIVERY_SALES ORDER NOTE] WHERE DELIVERY = '"+SO_ID+"'"
        exe4 = self.cur.execute(Delete_query4)
        self.cur.commit()
        print("ARTSANA_OUTBOUND_DELIVERY_SALES ORDER NOTE Deleted Successfully",SO_ID)


    def dbo_SALES_ORDER(self):          # 5 = SALES_ORDER

        sql_query5 = pd.read_sql_query("select * from [dbo].[SALES_ORDER] where SO_ID = '"+SO_ID+"'",self.conx)
        df = pd.DataFrame(sql_query5)
        df.to_csv (r'C:\Users\Emiza\Desktop\BACKUP_ARTSANA_SO\Backup_SALES_ORDER.csv', index = False)
        print("SALES_ORDER export complete")

        Delete_query5 = "DELETE FROM SALES_ORDER WHERE SO_ID = '"+SO_ID+"'"
        exe5 = self.cur.execute(Delete_query5)
        self.cur.commit()
        print("SALES_ORDER Deleted Successfully",SO_ID)


    def dbo_SALES_ORDER_HISTORY(self):  # 6 = SALES_ORDER_HISTORY

        sql_query6 = pd.read_sql_query("select * from [dbo].[SALES_ORDER_HISTORY] where SO_ID = '"+SO_ID+"'",self.conx)
        df = pd.DataFrame(sql_query6)
        df.to_csv (r'C:\Users\Emiza\Desktop\BACKUP_ARTSANA_SO\Backup_SALES_ORDER_HISTORY.csv', index = False)
        print("SALES_ORDER_HISTORY export complete")

        Delete_query6 = "DELETE FROM [SALES_ORDER_HISTORY] WHERE SO_ID = '"+SO_ID+"'"
        exe6 = self.cur.execute(Delete_query6)
        self.cur.commit()
        print("SALES_ORDER_HISTORY Deleted Successfully",SO_ID)


    def dbo_SALES_ORDER_LINES(self):    # 7 = SALES_ORDER_LINES

        sql_query7 = pd.read_sql_query("select * from [dbo].[SALES_ORDER_LINES] where SO_ID = '"+SO_ID+"'",self.conx)
        df = pd.DataFrame(sql_query7)
        df.to_csv (r'C:\Users\Emiza\Desktop\BACKUP_ARTSANA_SO\Backup_SALES_ORDER_LINES.csv', index = False)
        print("SALES_ORDER_LINES export complete")

        Delete_query7 = "DELETE FROM [SALES_ORDER_LINES] WHERE SO_ID = '"+SO_ID+"'"
        exe7 = self.cur.execute(Delete_query7)
        self.cur.commit()
        print("SALES_ORDER_LINES Deleted Successfully",SO_ID)


    def dbo_SALES_ORDER_LINES_HISTORY(self): # 8 = SALES_ORDER_LINES_HISTORY

        sql_query8 = pd.read_sql_query("select * from [dbo].[SALES_ORDER_LINES_HISTORY] where SO_ID = '"+SO_ID+"'",self.conx)
        df = pd.DataFrame(sql_query8)
        df.to_csv (r'C:\Users\Emiza\Desktop\BACKUP_ARTSANA_SO\Backup_SALES_ORDER_LINES_HISTORY.csv', index = False)
        print("SALES_ORDER_LINES_HISTORY export complete")

        Delete_query8  = "DELETE FROM [SALES_ORDER_LINES_HISTORY] WHERE SO_ID = '"+SO_ID+"'"
        exe7 = self.cur.execute(Delete_query8)
        self.cur.commit()
        print("SALES_ORDER_LINES_HISTORY Deleted Successfully",SO_ID)


    def dbo_SALES_ORDER_LINES_TRAIL(self):  # 9 = SALES_ORDER_LINES_TRAIL

        sql_query9 = pd.read_sql_query("select * from [dbo].[SALES_ORDER_LINES_TRAIL] where SO_ID = '"+SO_ID+"'",self.conx)
        df = pd.DataFrame(sql_query9)
        df.to_csv (r'C:\Users\Emiza\Desktop\BACKUP_ARTSANA_SO\Backup_SALES_ORDER_LINES_TRAIL.csv', index = False)
        print("SALES_ORDER_LINES_TRAIL export complete")

        Delete_query9  = "DELETE FROM [SALES_ORDER_LINES_TRAIL] WHERE SO_ID = '"+SO_ID+"'"
        exe7 = self.cur.execute(Delete_query9)
        self.cur.commit()
        print("SALES_ORDER_LINES_TRAIL Deleted Successfully",SO_ID)


a1 = Delete_SO()

a1.Header_query()
a1.Delivery_Item()
a1.Delhivery_Qty()
a1.Delhivery_SO()

a1.dbo_SALES_ORDER()
a1.dbo_SALES_ORDER_HISTORY()
a1.dbo_SALES_ORDER_LINES()
a1.dbo_SALES_ORDER_LINES_HISTORY()
a1.dbo_SALES_ORDER_LINES_TRAIL()

