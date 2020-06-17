package org.inceptez.spark.hackathon

class allMethods extends java.io.Serializable
{
  //Usecase - 24. Custom Method creation: Create a package (org.inceptez.hack), class (allmethods),method (remspecialchar)
    /*
      Hint: First create the function directly and then later add inside pkg, class etc..
      a. Method should take 1 string argument and 1 return of type string
      b. Method should remove all special characters and numbers 0 to 9 - ? , / _ ( ) [ ]
      Hint: Use replaceAll function, usage of [] symbol should use \\ escape sequence.
      c. For eg. If I pass to the method value as Pathway - 2X (with dental) it has to
      return Pathway X with dental as output.
		*/
  def remspecialchar(inputStr:String):String = 
  {
    return inputStr.replaceAll("[^a-zA-Z\\ ]", "")
  }
  
  def left(inpStr:String,len:Int):String =
  {
    return inpStr.substring(0, len)
  }

}