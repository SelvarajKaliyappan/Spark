package com.inceptez.datasecurity

object singleobject {
  
  def main(args : Array[String])
  {
    /*
      37. Create a scala object namely singleobject, create a main method, create objects like
  		val objmask=new com.inceptez.datasecurity.mask;
  		val objencode=new com.inceptez.datasecurity.endecode;
  		
  		38. Create an array with 3 names like Array(“arun”,”ram kumar”,”yoga murthy”), loop the array
  		elements, apply hashMask(name) for all 3 elements and println of the masked values.
  		
  		39. Loop the array created in above step and apply the revEncode(name) for all 3 elements and
  	  println of the encoded values.
     */
    
    val objmask = new com.inceptez.datasecurity.mask
    val objencode = new com.inceptez.datasecurity.endecode
    
    val arr = Array("arun","ram kumar","yoga murthy")
    println("---------------------------------------------------------")
      
    for ( i <- arr)
    {
      println("The mask value of " +i, " is " +objmask.hashMask(i))
      println("The Encode value of " +i, " is " +objmask.revEncode(i))
      println("The Decode value of " +objmask.revEncode(i), " is " +objencode.revDecode(objmask.revEncode(i)))
      println("---------------------------------------------------------")
    }
    
  }
  
}