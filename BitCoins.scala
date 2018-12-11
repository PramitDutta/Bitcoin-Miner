/**
 * Created by Pramit on 12/09/2015.
 */
import java.security.MessageDigest
import akka.actor.{Actor, ActorSystem, Props, actorRef2Scala}
import akka.routing.RoundRobinRouter
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/*  to start the Mining Process */
case class Mine(no_of_zeros : Integer)
/* Message from the Master to Miner assigning work and number of zeros as input*/
case class DistributeWork(noOfZeros : Integer)
/*message to collect coins and display them after mining is completed*/
case class MiningCompleted(inputsProcessed :Integer)
/*to store the bitcoins received from the miners after each miner unit is processed*/
case class CoinsMined(bitcoins : ArrayBuffer[String])

/*main object to start the master when the number of zeros is given as cmdline argument or to start the
remote worker if ip address is given as cmdline argument */

object Project1 extends App {
  var UserInput: String = args(0)
  if (UserInput.contains('.')) {
    // Assigning Remote Worker
    val miner = ActorSystem("MinerSystem").actorOf(Props[Miner].withRouter(RoundRobinRouter(nrOfInstances =6)))
    for (n <- 1 to 6)
      miner ! UserInput
  }
  else{
    //Calling Master*
    var noOfZeros = args(0).toInt
    val system = ActorSystem("MasterSystem")
    val master = system.actorOf(Props[Master],name="Master")
    master ! DistributeWork(noOfZeros)
    println("Master is invoked")
  }
}

class Master extends Actor {

  private var no_of_zeros: Integer = 0
  private var no_of_coins: Integer = 0
  private var countWorker: Integer = 0
  private var total_inputprocessed: Integer = 0
  private var numberofActors: Integer = 0
  private var total_CoinsMined: ArrayBuffer[String] = ArrayBuffer[String]()


  def receive = {
    /* Logic to assign Miner and distribute work among Miner Actors*/
    case DistributeWork(noOfZeros: Integer) => {
      no_of_zeros = noOfZeros
      val processes = Runtime.getRuntime().availableProcessors();
      val actorCount = 3 * processes
      numberofActors += actorCount
      println("Miner actors are assigned work")
      val miner = context.actorOf(Props[Miner].withRouter(RoundRobinRouter(nrOfInstances = actorCount)))
      for (n <- 1 to actorCount)
        miner ! Mine(no_of_zeros)
    }
      /* Coins mined by the Miners are collected */
    case CoinsMined(bitcoins: ArrayBuffer[String]) => {

      total_CoinsMined ++= bitcoins
      //println("Miners reported progress about mining")
    }
      /* After the mining work is completed by the Miners, bitcoins and other details are displayed */
    case MiningCompleted(inputstringsProcessed: Integer) => {
      countWorker += 1
      total_inputprocessed += inputstringsProcessed
      if (countWorker == numberofActors) {
        println("Number of workers : " + numberofActors)
        println("Number of inputs processed : " + total_inputprocessed)
        total_CoinsMined = total_CoinsMined.distinct
        for (i <- 0 until total_CoinsMined.length)
          println((i + 1) + " " + total_CoinsMined(i))
        println("Number of bitcoins found : " + total_CoinsMined.length)
        context.system.shutdown()
      }
    }
      /* to start 8 miners in the remote host */
    case "InvokeRemote" => {
      println("Remote Worker Invoked")
      numberofActors += 1
      sender ! Mine(no_of_zeros)

    }
  }
}

class Miner extends Actor {
  def receive = {
    /*  As Input String Gatorlink ID is concatenated with two random strings and given to SHA256 of MessageDigest library.
	   The hash string found is compared for number of zeros and saved in an ArrayBuffer.
	   The mined bitcoins are passed to the master after a mine unit of 1000000 is processed.
	   The miner stops computation when the time exceeds 240s */
    case Mine(no_of_zeros: Integer) => {
      var bitcoins:ArrayBuffer[String]=  ArrayBuffer[String]()

      var inputstringsProcessed:Integer=0
      val minerUnit=10000
      val startTime=System.currentTimeMillis()
      while(System.currentTimeMillis()-startTime<360000){
        var string:String = "pdutta04"+Random.alphanumeric.take(5).mkString+Random.alphanumeric.take(5).mkString
        val sha = MessageDigest.getInstance("SHA-256")
        sha.update(string.getBytes("UTF-8"))
        val digest = sha.digest();
        val hexString = new StringBuffer();
        for ( j <- 0 to digest.length-1) {
          val hex = Integer.toHexString(0xff & digest(j));
          if (hex.length() == 1) hexString.append('0');
          hexString.append(hex);
        }
        var extracted_val:String=hexString.substring(0,no_of_zeros)
        var comparison_val="0"*no_of_zeros
        if(extracted_val.equals(comparison_val)){
          bitcoins+=string+" "+hexString
        }
          inputstringsProcessed+=1
          if(inputstringsProcessed%minerUnit==0)
            sender ! CoinsMined(bitcoins)
        }
      sender ! MiningCompleted(inputstringsProcessed)
    }
    case ipaddress:String => {
      println("Remote Worker Mining")
      val master=context.actorSelection("akka.tcp://MasterSystem@"+ipaddress+":5152/user/Master")
      master! "InvokeRemote"
    }
  }
}
