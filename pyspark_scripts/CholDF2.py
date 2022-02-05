
import collections
from operator import add



Patient = collections.namedtuple("Patient", "id name address phone_number")

BloodTest = collections.namedtuple("BloodTest", "id id_patient code date description result")



patient1=Patient(1, "Alice", "Pza L. Da Vinci 32", "02 2399")
patient2=Patient(2, "Bob", "Via Golgi 40", "02 23992")
bloodTest1=BloodTest(1,1,1, "17/2/2017", "Glucose",94)
bloodTest2=BloodTest(2,1,2, "17/2/2017", "Cholesterol",140)
bloodTest3=BloodTest(3,2,1, "16/2/2017", "Glucose",83)
bloodTest4=BloodTest(4,2,2, "16/2/2017", "Cholesterol",238)


patientsRDD=sc.parallelize([patient1,patient2])
testsRDD=sc.parallelize([bloodTest1,bloodTest2,bloodTest3,bloodTest4])









patientsDF=patientsRDD.toDF(["id", "name", "address", "phone_number"])
patientsDF.show()



testsDF=testsRDD.toDF(["id", "id_patient", "code", "date", "description", "result"])
testsDF.show()




#Provide the name of patients whose cholesterol is greater than 220

filteredExams = testsDF.where("description = 'Cholesterol' and result > 220")
filteredExams.show(5, False)


filteredPatients = patientsDF.\
join(filteredExams, (patientsDF.id==filteredExams.id_patient))
filteredPatients.select("name", "result").show(1, False)

#min valore colesterolo

from pyspark.sql.functions import min

minCholesterol = testsDF.where("description='Cholesterol'").\
select(min("result")).collect()[0][0]
print(minCholesterol)

#paziente col min valore colesterolo

minExam = testsDF.where("result = " + str(minCholesterol))
minExamPatient = minExam.select("id_patient").collect()[0][0]
minPatient = patientsDF.where("id = " + str(minExamPatient))
minPatient.show()
