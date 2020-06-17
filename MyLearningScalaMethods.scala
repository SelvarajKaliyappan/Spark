package org.inceptez.scalaMethods

class MyLearningScalaMethods {
  
  //Usecase-14 - Method for divisible by any no
  def divisibleByNo(n : Int, divi : Int = 4) : Int = { return n % divi } 
  
  //Usecase-14 - Method to return data type of specific variable.
  def getType(v : Any) : Any = { return v.getClass() }
  
  //Usecase-14 - method to return Odd/Even number
  def isEven(num : Int) : Int = { return num % 2 }
  
  //Usecase-14 - method to check a string is palindrome or not
  def checkPalindrome(givenStr : String) : String = 
  {
    if (givenStr.toLowerCase() == givenStr.toLowerCase().reverse)
    {
      return "The given string is - " + givenStr + " and it is a PALINDROME!"
    }
    else
    {
      "The given string is - " + givenStr + " and it is NOT a PALINDROME!"
    }
  }
  
  //Usecase-14 - Method to get Course fee from given Course Info
  def getCourseFee(courseInfo : String) : Unit =
  {
    if(courseInfo.toLowerCase() == "bigdata") 
    { 
      println("Course enquired is - " + courseInfo, " and fees is - 25000")
    } 
    else if (courseInfo.toLowerCase() == "spark") 
    {
      println("Course enquired is - " + courseInfo , " and fees is - 15000")
    }
    else if (courseInfo.toLowerCase().contains("machine")) 
    { 
      println("Course enquired is - " + courseInfo, " and fees is - 35000")
    } 
    else if (courseInfo.toLowerCase().contains("deep")) 
    { 
      println("Course enquired is - " + courseInfo, " and fees is - 45000")
    } 
  }
  
  //Usecase-14 - Method to find greatest no using If... Else.
  def findGreatestNo(a:Int,b:Int,c:Int) : AnyVal = 
  {
    if ( (a > b) & (a > c) ) { return a } //check if a is greatest
    else if ( (b > a) & (b > c) ) { return b } //check if b is greatest 
    else { return c }   //else return c is greatest 
  }
  
  //Usecase-14 - Method to find greatest no using overloading.
  def findGreatestNo(lst: List[Int]) : Int = { return lst.max }
  
    //Usecase-15 - Method for calculator
  def calculator(val1 : Int, val2: Int, mathType : String) : Any = 
  {
    if( (mathType == "addition") || (mathType == "add") )
    {
      return val1 + val2
    }
    else if( (mathType == "subtraction") || (mathType == "sub") )
    {
      return val1 - val2
    }
    else if( (mathType == "multiplication") || (mathType == "mul") )
    {
      return val1 * val2
    }
    else if( (mathType == "division") || (mathType == "div") )
    {
      return (val1.toFloat / val2)
    }
    else
    {
      return 0
    }
  }

  //Usecase-16 - Method to get cube values, and use multiple return statement, also find which one is returning really.
  def getCube(x : Int) : Int = { x*x*x; return x*x*x ; return x*2; x*3 }
   
  //Usecase17 - Method to return multiple return types.
  def getEmpInfo(empId : Int) : (String, Int, Byte) = 
  {
    var name : String = ""
    var salary : Int = 0
    var age : Byte = 0
    if (empId == 1212)
    {
      name = "Selvaraj Kaliyappan"
      salary = 25000
      age = 39
    }
    else
    {
      name = "Kumar Raj"
      salary = 35000
      age = 38
    }
    return (name,salary,age)
  }
  
  //Usecase-18 - Method to get datatype of the given value. 
  def getDataType(v : Any) 
  {
   v match
   {
    case s: Int => println("The given value is " +s, " and it's data type is Integer")
    case s: String => println("The given value is " +s, " and it's data type is String")
    case s: Character => println("The given value is " +s, " and it's data type is Character")
    case s: Float => println("The given value is " +s, " and it's data type is Float")
    case s: Boolean => println("The given value is " +s, " and it's data type is Boolean")
    }
  }
  
  //Usecase-19 Exception handling inside the method
  def Metexception(numerator:Int,denominator:Int):Any=
  {
    try
    {
      println("The reamining value is " + (numerator / denominator))
    }
    catch
    {
        case a: java.lang.ArithmeticException => 
        {
            println("Divisible by Zero error occured, please check your input")
        }
        case b: java.lang.Exception => 
        {
            println("Some exception occured, pls look in to your input")
        }  
    }
  }

  //Usecase-25, Method to find greatest number from Array
  def getGreatestNo(arr : Array[Int]) : Int = { return arr.max }
  
  //Usecase-26, Method to sum up from list
  def listSumUp(lst : List[Int]) : Int = { return lst.sum }
  
  //Usecase-27, Method to count the no of element from list
  def getEleCnt(lst : List[String]) : Int = { return lst.size }
  
  //Usecase-28,29,30,31, Method to find capital of india
  def getCountryCap(country : String, retType : String = "string") : Any =
  {
    try
    {
      //Store Countries and its capital using Map
      var m = Map("China" -> "Beijing", "India" -> "New Delhi", "USA" -> "Washington", "UK" -> "London")
      
      if ( (country != "All") & (retType == "string") ) { return m(country) }
      else if ( (country == "All") & (retType == "Array") ) 
      { 
        var arr = m.toArray
        return arr.foreach(i => println(i._1)) 
      }
      else if ( (country == "All") & (retType == "Set") ) 
      { 
        var set = m.toSet
        return set.foreach(i => println(i._1)) 
      }
    }
    catch
    {
      case a: java.util.NoSuchElementException => { return "The given Country is not correct, pls check and share correct info!"}
      case b: java.lang.Exception => { return "Some exception occured, pls look in to your input!" }
    }  
  }
  
  def getCountry() =
  {
      var m = Map("China" -> "Beijing", "India" -> "New Delhi", "USA" -> "Washington", "UK" -> "London")
      //println(m.keys)
      m.keys.foreach { println }
  }
}