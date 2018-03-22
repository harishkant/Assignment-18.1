# Assignment-18.1

val holiday = sc.textFile("/home/acadgild/Downloads/S18_Dataset_Holidays.txt")
val HolidaString = "UserID From To TravelBy Distance Year"
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType, StructField, StringType};
val holidayschema = StructType(HolidaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
val HolidayRDD = holiday.map(_.split(",")).map(e ⇒ Row(e(0), e(1), e(2),e(3),e(4),e(5)))
val HolidayDF = sqlContext.createDataFrame(HolidayRDD, holidayschema)
HolidayDF.registerTempTable("Holiday")
sqlContext.sql("SELECT * FROM Holiday").show()



val Transport = sc.textFile("/home/acadgild/Downloads/S18_Dataset_Transport.txt")
val TravelString = "TravelBy Fare"
val Travelschema = StructType(TravelString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
val TransportRDD = Transport.map(_.split(",")).map(e ⇒ Row(e(0), e(1)))
val TransportDF = sqlContext.createDataFrame(TransportRDD, Travelschema)
TransportDF.registerTempTable("Transport")
sqlContext.sql("SELECT * FROM Transport").show()


val User = sc.textFile("/home/acadgild/Downloads/S18_Dataset_User_details.txt")
val UserString = "UserID UserName Age"
val Userschema = StructType(UserString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
val UserRDD = User.map(_.split(",")).map(e ⇒ Row(e(0), e(1),e(2)))
val UserDF = sqlContext.createDataFrame(UserRDD, Userschema)
UserDF.registerTempTable("User")
sqlContext.sql("SELECT * FROM User").show()




val FinalTable=sqlContext.sql("SELECT H.UserID,H.From,H.To,H.TravelBy,H.Distance,H.Year,U.UserName,U.Age,T.Fare FROM Holiday H LEFT JOIN User U on U.UserID=H.UserID LEFT JOIN Transport T on H.TravelBy=T.TravelBy")

FinalTable.registerTempTable("Final")



Question 1: What is the distribution of the total number of air-travelers per year
sqlContext.sql("SELECT Year,count(*) TravelCount FROM Final GROUP BY Year").show()

![screenshot-1](https://user-images.githubusercontent.com/34162166/37763836-1edfa310-2de6-11e8-868c-11e367b6a732.png)

Question 2: What is the total air distance covered by each user per Year
sqlContext.sql("SELECT Year,UserID,SUM(Distance) TotalDistance FROM Final GROUP BY Year,UserID").show()


![screenshot-2](https://user-images.githubusercontent.com/34162166/37763837-1f2cd018-2de6-11e8-8e54-d9e3a2501bc9.png)

Question 3: Which User has travelled the largest distance till date.
sqlContext.sql("SELECT UserID,UserName,SUM(Distance) TotalDistance FROM Final GROUP BY UserID,UserName").show()


![screenshot-3](https://user-images.githubusercontent.com/34162166/37763839-1f75add8-2de6-11e8-9566-6826f7d04017.png)

Question 4: What is the most Prefered destination for all Users
var PreferDest=sqlContext.sql("SELECT From,To,Count(UserID) TotalTravel FROM Final GROUP BY From,To")
PreferDest.registerTempTable("PreferDestination")
sqlContext.sql("SELECT * from PreferDestination where TotalTravel>2").show()



![screenshot-4](https://user-images.githubusercontent.com/34162166/37763841-1fba6478-2de6-11e8-9449-3c228346d178.png)


















