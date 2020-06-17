package com.inceptez.datasecurity

class mask {
  /*
   	33. Create a package namely com.inceptez.datasecurity
		34. Inside the above package, Create a class called mask and one more class called endecode
		35. Inside the class mask create a private val as addhash=100 and a method
		36. Inside the class mask create a private val as prefixstr=”aix” and a method
		revEncode(str:String):String={return the prefixstr+reverse of str value}
		*/
  private val addhash = 100
  private val prefixstr = "aix"
  
  def hashMask(str:String):Int={return (str+addhash).hashCode()}
  def revEncode(str:String):String={return prefixstr+str.reverse}
}