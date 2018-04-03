package streaming

import java.io.PrintWriter
import java.net.ServerSocket

import scala.collection.mutable.ListBuffer
import scala.util.Random

object GenerateChar {

  def generateContext(index : Int) :String ={
    val charList = ListBuffer[Char]()
    for(i <- 65 to 90)
      charList += i.toChar
    val charArray = charList.toArray
    charArray(index).toString

  }

  def index = {
    val random = new Random()
    random.nextInt(7)
  }

  def main(args : Array[String]){



    val listener = new ServerSocket(9998)
    while (true){
      val socket = listener.accept()
      new Thread(){
        override def run() = {
          println("Got client connected from :" + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream,true)
          while (true){
            Thread.sleep(500)
            val context = generateContext(index)
            println(context)
            out.write(context + '\n')
            out.flush()
          }
          socket.close()
        }
      }.start()

    }

  }




}
