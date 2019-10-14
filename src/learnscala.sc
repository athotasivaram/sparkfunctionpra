val name = "sivaram"
val check= {
  if (name.length <= 3) "pls enter min 4 chars"
  else name.toUpperCase
}

//match
/*

val mat = name match  {
  case name if(name.length<=5) => "pls enter min 4 characters"
  case name if(name.length>20) => "pls enter chars between 4-20 only "
  case _ => name.toUpperCase
}
*/


val rs = {
  if (name=="sivaram")"do admin task"
  else if(name=="kumar") "do testing tasks"
  else if(name=="super") "u r in bench"
  else "you are not in this project"
}

def m(name:String) = name match {
  case "sivaram" => "pls do it this job"
  case "kumar" => "pls joining to day"
  case "rajesh" => "pls verify this project"
  case _  => "pls do somthing"

}

val age=44
def  ageoff(age:Int) = {
    if(age>=1 && age<=12) "infints"
  else if(age>12 && age<=18) "minor"
  else if (age>18 && age<60) "major"
  else if (age>=60) "old age"
  else "pls check your age"
}

val ageoffer = age match  {
  case age if(age>=1 && age<=12) => "your young age "
  case age if(age>12 && age<18)=>" your minor"
  case age if(age>18 && age<60)=> "your major"
  case _ =>"pls chack input values"
}