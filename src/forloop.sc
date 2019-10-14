//val names = Array("siva","kumar","chandu","ramu")

def pro(names:Array[String]) = for(x<-names if(x.length>3)) yield x match {
  case a if(a.contains(" ")) => a.replaceAll(" ","")
case _ => x.toUpperCase
}