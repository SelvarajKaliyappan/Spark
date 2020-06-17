package org.inceptez.scalaprograms
import scala.collection.mutable.ListBuffer

object ScalaMyLearningUsecases {
  //Main method
  //Author : Selvaraj K
  //Date : 23-Mar-2020
  //Description : This object is created to perform different Scala use cases
  
  //create object for MyLearningScalaMethods class as objMLSM.
  val objMLSM = new org.inceptez.scalaMethods.MyLearningScalaMethods
  
  //Case class is to store Student info
  case class student(stuName : String, Tamil : Byte, English : Byte, Maths : Byte);
  
  def main(args:Array[String])
  {
    //Usecase 1
    //1. Create 2 val types with x as 100 & y as 10 respectively and find the Multiplication and division of both 
    //and store in some val as z and z1.
    println("****************************************************************************************************************")
    println("Usecase-1 example - Multiply and division by 2 nos")
    val x : Int = 100
    val y : Int = 10
    val z : Int = x * y
    val z1 : Int = x / y
    println("The value of x,y are",x,y)
    println("Multiplication of x & y, and value of z is " +z )
    println("Division of x & t, and value of z1 is " + z1)
    println("****************************************************************************************************************")
    println("                                              ")
    
    //Usecase2
    //2. Create a as 2000 and find the division of a by y created in step 1 and reassign a with the divided result (200).
    println("Usecase-2 example - Check division of no by var type")
    var a = 2000
    a = a / y
    println("The value of a is " +a)
    println("Division of a by y, and value of a now is " +a )
    println("****************************************************************************************************************")
    println("                                              ")
    
    //Usecase3
    //3. Create a val type with x:Int=100, then assign the x to val y, but the datatype of y has to be String. 
    //(think about using some function like toString)
    println("Usecase-3 example - Check data type and reassign")
    val x1 : Int = 100
    val y1 : String = x1.toString()
    println("Value of x1 is " +x1, " and type is " +x1.getClass())
    println("Value of y1 is " +y1, " and type is " +y1.getClass())
    println("****************************************************************************************************************")
    println("                                              ")
    
    //Usecase4
    //4. Try only in REPL for now - Create a val type sc1 and assign sc into it and 
    //also try assigning sc1 defined as AnyRef/Any and check the type of the sc1 using getClass function.
    
    //Usecase5
    //Static definition and Dynamic inference
    //5. Create some var and val and prove static definition by re-assigning var with different data type 
    //and dynamic inference by displaying the data type respectively.
    //Static data type
    println("Usecase-5 example - Static and Dynamic data type")
    val age : Int = 38
    val name : String = "Selvaraj"
    val preaddress : Any = "Villa No 21, DABC Gardenia, Polacheri, chennai-600127"
    println("Name is " + name, " age is " + age, " present address is " + preaddress)
    //Reassign permanent address
    println("Reassign example")
    var peraddress : String = preaddress.toString()
    peraddress = "82/4 Kallathi Mudukku street, Tirunelveli, 627002"
    println("Name is " + name, " age is " + age, " permenant address is " + peraddress)
    //dynamic inferred example
    val s1 = " - My favorite sports is Cricket"
    val s2 = 100
    println("Dynamic inferred example")
    println("The value of s1 is " +s1 , " and type is " + s1.getClass())
    println("The value of s2 is " +s2 , " and type is " + s2.getClass())
    println("****************************************************************************************************************")
    println("                                              ")
    
    //Usecase6
    //6. Write a program to find the greatest of 3 numbers
    println("Usecase-6 example - Find greatest no from given nos")
    println("Greatest No example using individual value using If...Else...")
    val aa = 10; val bb=20; val cc=30
    val greatestNoiS = objMLSM.findGreatestNo(aa, bb, cc)
    println("The given no are",aa,bb,cc)
    println("The greatest no is - " +greatestNoiS)
    println("Greatest No example using LIST and Method overloading")
    val lst = List(20,3,18)
    val greatestNoiS_1 = objMLSM.findGreatestNo(lst)
    println("The given no are",lst)
    println("The greatest no is - " + greatestNoiS_1)
    println("****************************************************************************************************************")
    println("                                              ")
    
    //Usecase7
    //7. Write a nested if then else to print the course fees of if the student choose bigdata then check if bigdata 
    //then fees is 25000, if spark then fees is 15000, if the student chooses datascience then check 
    //if machinelearning then 35000, if deep learning then 45000.
    println("Usecase-7 example - Check course fees")
    println("Nested If...Else... example")
    var courseInfo = "Bigdata"
    objMLSM.getCourseFee(courseInfo)
    courseInfo = "SPARK"
    objMLSM.getCourseFee(courseInfo)
    courseInfo = "machine"
    objMLSM.getCourseFee(courseInfo)
    courseInfo = "DEEP"
    objMLSM.getCourseFee(courseInfo)
    println("****************************************************************************************************************")
    println("                                              ")
    
    //Usecase-8
    //8. Check whether the given string is palindrome or not (try to use some function like reverse). 
    //For eg: val x="madam" then print as "palindrome" else "non palindrome".
    println("Usecase-8 example - Check given no is palindrome or not")
    println("Palindrome example")
    var str : String = "Madam"
    println(objMLSM.checkPalindrome(str))
    str = "LIRIL"
    println(objMLSM.checkPalindrome(str))
    str = "selvaraj"
    println(objMLSM.checkPalindrome(str))
    println("****************************************************************************************************************")
    println("                                              ")
    
    //Usecase-9
    //9. Check whether the val x=100 is an integer or string. 
    //(try to use some functions like toString, toUpperCase etc to execute this use case)
    println("Usecase-9 example - Check the val x is Int or String")
    var x2 : Any = 100
    println("The value of x is - " +x2, " and the type is - " + objMLSM.getType(x2))
    x2 = "selvaraj".toUpperCase()
    println("The value of x is - " +x2, " and the type is - " + objMLSM.getType(x2))
    println("****************************************************************************************************************")
    println("                                              ")
    
    //Usecase-10
    //10. Write a program using while or for loop to print even numbers and odd numbers between any range of data 
    //as per your intention and also find the even and odd values between 5 and 20 
    //(even should be 6,8,10,12,14,16,18,20 and odd should be 5,7,9,11,13,15,17,19).
    println("Usecase-10 example - Find Odd or Even numbers")
    val lst1 = List.range(1, 21)
    println(lst1)
    var oddLst = ListBuffer[Int]()
    var evenLst = ListBuffer[Int]()
    for ( n <- lst1)
    {
      if (objMLSM.isEven(n) == 0)
      {
        evenLst+=n
      }
      else
      {
        oddLst+=n
      }
    }   
    println("The even numbers are " + evenLst)
    println("The odd numbers are " + oddLst)
    println("****************************************************************************************************************")
    println("                                              ")
    
    //Usecase-11
    //11. For loop to increment from 0 till 21 with the increment of 3, the result should be exactly 0,3,6,9,12,15,18
    println("Usecase-11 example - Increment by 3")
    val n = 21
    var increby3 = ListBuffer[Int]()
    for(i <- 0 to n by 3)
    {
      increby3+= i
    }
    println("The values are " + List.range(1,22))
    println("The increment for loop by 3 values are - " + increby3)
    println("****************************************************************************************************************")
    println("                                              ")
    
    //Usecase-12
    //12. Write a for or while loop to print the cube of 4, result should be 4*4*4=64 
    //(think of using some var type initiated outside the loop)
    println("Usecase-12 example - While or For loop for Cube")
    val n1 = 4
    for (i <- 1 to n1)
    {
      //Get cube value for specific number.
      /*if (i == 4)
      {
        println("The cube value for no " + i + " is - " + getCube(i))
      }*/
      
      //get cube value from 1 to N
      println("The cube value for no " + i + " is - " + objMLSM.getCube(i))
    }
    println("****************************************************************************************************************")
    println("                                              ")
    
    //Usecase-13
    //13. Write for/while loop for printing only the values in the range of 1 to 20 
    //which are divisible by 4 (don’t use by 4 in the for loop) rather use if condition to check the % of 4 
    //for every element in the loop achieve this.Result should be exactly like this 4,8,12,16,20.
    println("Usecase-13 example - While or For loop printing specific values")
    val n2 = 20
    //divisible by 4 default
    for(i <- 1 to n2)
    {
      if ( objMLSM.divisibleByNo(i) == 0 ) //only passed i value. the division value 4 is passed by default in method itself.
      {
        println("The number divisible by 4 between 1 and 20's are - " + i)
      }
    }
    
    //divisible by 6
    for(i <- 1 to n2)
    {
      if ( objMLSM.divisibleByNo(i,6) == 0 )
      {
        println("The number divisible by 6 between 1 and 20's are - " + i)
      }
    }
    println("****************************************************************************************************************")
    println("                                              ")
          
    //Usecase - 15
    /*  15. Write a method to create a calculator accepts 3 arguments and return type of any, 
    *  first 2 of integer and 3rd one is String, based on the 3rd argument value as add/sub/div/mul perform 
    *  either addition or subtraction or multiplication or division of values and 
    *  return the result to the calling environment.(for division think of using.toFloat or .toDouble conversion).*/
    println("Usecase-15 example - Calculator Method")
    val number1 = 5
    val number2 = 20
    var mathType = "ADDITION"
    println("Addition of two nos (" + number1 + "," +number2 +") is - " + objMLSM.calculator(number1,number2,mathType.toLowerCase()))
    println("Subtraction of two nos (" + number1 + "," +number2 +") is - " + objMLSM.calculator(number1,number2,"sub"))
    println("Multiplication of two nos (" + number1 + "," +number2 +") is - " + objMLSM.calculator(number1,number2,"mul"))
    println("Division of two nos (" + number1 + "," +number2 +") is - " + objMLSM.calculator(number1,number2,"division"))
    println("****************************************************************************************************************")
    println("                                              ")
    
    //Usecase-17
    //17. Try creating a method with multiple return types.
    println("Usecase-17 example - Calculator Method")
    val empid : Int = 1212
    println("The employe info of emp id " + empid + " is " + objMLSM.getEmpInfo(empid))
    println("The employe info of emp id 345 is " + objMLSM.getEmpInfo(345))
    println("****************************************************************************************************************")
    
    //Usecase-18
    //Write a program using case using pattern matching to find the datatype of a given value and
    //return either Float or string or Boolean or Char etc..
    println("Usecase-18 example - GetDataType Method")
    var value : Any = 1212
    objMLSM.getDataType(value)
    value = "Selvaraj"
    objMLSM.getDataType(value)
    value = 'T'
    objMLSM.getDataType(value)
    value = 12.45F
    objMLSM.getDataType(value)
    value = true
    objMLSM.getDataType(value)
    println("****************************************************************************************************************")
    
    //Usecase-19
    /*19. Create a method should accept 2 aruments and a return value
    metexception(numerator:Int,denominator:Int):Int={} , in the main block return the value of
    numerator/denominator
    for eg. If you call metexception(10,2) the return should be 5 but if you call as metexception(10,0)
    usually it throws exception, in case of exception we have to handle in the catch block where it
    should call the same metexception with the argument passed as (10,1) so the result will be 10. 
    */

    println("Usecase-19 example - Method Exception Method")
    objMLSM.Metexception(10,2)
    objMLSM.Metexception(10,0)
    objMLSM.Metexception(10,1)
    println("****************************************************************************************************************")
    
    //Usecase-20
    //20. Create an array, list and prove mutability and immutability and non-resizable properties.
    println("Usecase-20 example - Mutable and Immutable Array and List Example")
    val arr = Array("Selvaraj",39,"Tirunelveli,India,Tamilnadu-627006")
    println("Age is "+arr(1)," Name is " +arr(0)," Address is " +arr(2))
    
    println("Mutable Array example. i.e - Update the exisiting value on array")
    //update new address
    arr(2) = "Chennai, Polacheri - 600127"
    println("New address is " +arr(2))
    
    //println("Resizable Array example. i.e - Add phone number on array")
    //Add phone number in to existing Array. If you uncomment below 2 lines, it will throw error.
    //arr(3) = "9840913381"
    //println("Mobile number is " +arr(3))
    
    println("Immutable List example.")
    val lst2 = List(1,2,3,4)
    println(lst2)
    println("The value of list(2) is " +lst2(2))
    //try to add value 5 in list, but it will throw error if you uncomment below line.
    //lst2+=5
    println("****************************************************************************************************************")
    
    //Usecase-21
    //21. Create arraybuffer from scala.collection.mutable package and prove mutability and
    //immutability and resizable properties.
    println("Usecase-21 - Mutable Array Buffer and List Buffer Example")
    
    val arrBuf = scala.collection.mutable.ArrayBuffer(1,2,3,4)
    val lstBuf = scala.collection.mutable.ListBuffer(1,2,3,4)
    println("The Array buffer values are " +arrBuf)
    println("The List buffer values are " +lstBuf)
    println("-------------------------------------")
    println("Resize the Array and List buffer.")
    println("-------------------------------------")
    arrBuf+=5
    lstBuf+=5
    println("Resizable Array buffer values are " +arrBuf)
    println("Resizable List buffer values are " +lstBuf)
    println("-------------------------------------")
    println("Update the Array and List buffer.")
    println("-------------------------------------")
    arrBuf(4)=41
    lstBuf(4)=71
    println("Updated Array buffer values are " +arrBuf)
    println("Updated List buffer values are " +lstBuf)
    println("****************************************************************************************************************")
    
    //Usecase-22
    //22. Create a tuple of 4 fields and access the 2nd and 4th fields and store in another tuple.
    println("Usecase-22 - Tuple Example")
    var stu = ("Selvaraj",95,100,99)
    println("The student and his marks(Tamil,English,Mathematics) are " +stu)
    var tamilMark = stu._2
    var mathsMark = stu._4
    println("Tamil marks is " +tamilMark, " and Maths mark is " +mathsMark)
    println("****************************************************************************************************************")
   
    //Usecase-23
    //23. Find the maximum value out of (2,3,1,5,4) elements in the array.
    println("Usecase-23 Example - Find max value from Array")
    var arr1 = Array(2,3,1,5,4)
    println("The give values in array are " +arr1.toList)
    println("The biggest value from array is " +arr1.max)
    println("****************************************************************************************************************")
  
    //Usecase-24
    //24. Find the max and min value of (2,3,1,5,4) elements in the array and store these 2 values in
    //another array.
    println("Usecase-24 Example - Find min & max value from Array and store in another arry")
    var arr2 = Array(2,3,1,5,4)
    println("The give values in array are " +arr1.toList)
    var minArr = arr2.min
    var maxArr = arr2.max
    var newArr = Array(minArr,maxArr)
    println("The min and max values of give array are " +newArr.toList)
    println("****************************************************************************************************************")
    
    //Usecase-25
    //25. Create a method to find the highest value in the given array if the array is non empty and print
    //it, you must pass array as an argument to the method.
    println("Usecase-25 Example - Find highest value from Array using Method")
    var arr3 = Array(20,3,17,75,24)
    println("The give values in array are " +arr3.toList)
    println("The biggest value from array is " +objMLSM.getGreatestNo(arr3))
    println("****************************************************************************************************************")
  
    //Usecase-26
    //26. Write a program to create an Int List with 5 different values using range and sum all the values
    println("Usecase-26 Example - Sum of all values in list using Method")
    var lst3 = List.range(1, 100,20)
    println("The give values in list are " +lst3)
    println("The sum of value from List range is " +objMLSM.listSumUp(lst3))
    println("****************************************************************************************************************")
  
    //Usecase-27
    //27. Write a program to create string list to store the values of Spark,Scala,Python,Java,Hadoop and
    //count the number of elements in the List
    println("Usecase-27 Example - Count the number of elements in the List using Method")
    var lst4 = List("Spark","Scala","Python","Java","Hadoop")
    println("The give values in list are " +lst4)
    println("The no of element in the List is " +objMLSM.getEleCnt(lst4))
    println("****************************************************************************************************************")
    
    
    //Usecase-28,29,30 & 31
    //28. Write a program to store (China,Beijing),(India,New Delhi),(USA,Washington),(UK,London) using Map
    //29. Find the capital of India
    //30. Take only countries and store in an array and use foreach and println to print line by line of elements.
    //31. Take only countries and store in an set and use foreach and println to print line by line of elements.
    println("Usecase-28,29,30 & 31 Example - Country and Capital using Method")
    var country = "India"
    println("Capital of " +country + " is " +objMLSM.getCountryCap(country))
    println("----------------------------------------")
    println("Countries listing down by Array example")
    println("----------------------------------------")
    var allCntryArray = objMLSM.getCountryCap("All","Array")
    println("----------------------------------------")
    println("Countries listing down by Set example")
    println("----------------------------------------")
    var allCntrySet = objMLSM.getCountryCap("All","Set")
    println("****************************************************************************************************************")
    
    //Usecase-32
    //32. Create a case class and apply the respective column name and datatype for the tuple created in step 22.
    println("Usecase-32 - Case class Example")
    var stu1 = student("Selvaraj",95,100,99)
    println("Case class - " +stu1)
    println("Student Name " +stu1.stuName, " Tamil - " +stu1.Tamil, " English - "+stu1.English, " Maths - "+stu1.Maths)
    println("****************************************************************************************************************")
    
 }
}