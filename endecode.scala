package com.inceptez.datasecurity

class endecode {
  
  def revDecode (revStr : String) : String = 
  {
    /*
    	40. If possible create a decode function inside endecode class as revDecode and write a logic to
    	decode the encoded string in step 39.
		*/
    return revStr.reverse.replace("xia", "") 
   }
}