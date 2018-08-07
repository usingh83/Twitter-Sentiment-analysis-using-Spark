package lab
package lab 
import com.koddi.geocoder.{Geocoder, ResponseParser}

object Geo {
  def getLatLon(Address:String):com.koddi.geocoder.Location={
    // Next lets create an anonymous Geocoder client
    val client = Geocoder.create("AIXXXXCTV_2tXeluPONcixDLb-6xrpYHpiYv_UE")
    // We can now use our Geocoder as we see fit   
    if(Address!=None){
    print(Address)
    if(Address==null) return com.koddi.geocoder.Location.apply(0, 0)
    val results = client.lookup(Address) 
    print(results)
    if(results==List()) return com.koddi.geocoder.Location.apply(0, 0)
    val location = results.head.geometry.location
    return location
    }
    else return client.lookup("Antarctica").head.geometry.location
    
  }
}