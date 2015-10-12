import scala.io.StdIn.{readLine,readInt}

/**
 * Created by karthik on 10/11/15.
 */
trait KVStore {
  var kvStore: Map[String, String] = Map()
  var valueStore: Map[String, Set[String]] = Map()

  def get(key: String): String = kvStore.getOrElse(key, "NULL")

  def set(key: String, value: String) = {
    kvStore.get(key).foreach(_ => unSet(key))
    kvStore = kvStore.updated(key, value)

    valueStore.get(value) match {
      case Some(keys) => valueStore = valueStore.updated(value, keys.+(key))
      case None => valueStore = valueStore.updated(value, Set(key))
    }
  }

  def numEqualTo(value: String) = {
    valueStore.getOrElse(value, Set()).size
  }

  def unSet(key: String) = {
    kvStore.get(key) match {
      case Some(value) => {
        val matchingKeys = valueStore.get(value).get
        val newMatchingKeys = matchingKeys - key
        valueStore = valueStore.updated(value, newMatchingKeys)
        kvStore = kvStore - key
      }
      case None => {}
    }
  }

  def entrySet = kvStore.toSet
}

class MyRedis extends KVStore {
  var runningTransactions: List[TransactionBlock] = Nil

  override def set(key: String, value: String) = {
    if(runningTransactions.nonEmpty) {
      val recentTransaction = runningTransactions.head
      recentTransaction.addCommand(SetCommand(key, value))
    } else {
      super.set(key, value)
    }
  }

  override def get(key: String): String = {
    runningTransactions.headOption.flatMap(transaction => transaction.getCommand(key)) match {
      case Some(updateCommand) => updateCommand match {
        case SetCommand(key, value) => value
        case UnSetCommand(key) => "NULL"
      }
      case None => super.get(key)
    }
  }


  // Not O(logN) but is constant in number of keys in the db. Depends on the number of keys associated with the tr
  //Since that is small- assuming it is O(1)
  override def numEqualTo(value: String) = {
    if(runningTransactions.isEmpty) {
      valueStore.getOrElse(value, Set()).size
    } else {
      val keysInTransact = runningTransactions.head.commandSet
      var keysMatchingValue = valueStore.getOrElse(value, Set())
      keysInTransact.foreach { command =>
        command match {
          case SetCommand(key, tempVal) => {
            if(tempVal == value && !keysMatchingValue.contains(key)) {
              keysMatchingValue = keysMatchingValue + key
            } else if(keysMatchingValue.contains(key) && tempVal != value) {
              keysMatchingValue = keysMatchingValue - key
            }
          }
          case UnSetCommand(key) => {
            if(keysMatchingValue.contains(key)) {
              keysMatchingValue = keysMatchingValue - key
            }
          }
        }
      }
      keysMatchingValue.size
    }
  }

  // Remove entry from the KV Store and remove entry for key
  override def unSet(key: String) = {
    if(runningTransactions.nonEmpty) {
      val recentTransaction = runningTransactions.head
      recentTransaction.addCommand(UnSetCommand(key))
    } else {
      super.unSet(key)
    }
  }

  def beginTransaction() = {
    runningTransactions = runningTransactions.headOption match {
      case Some(recentTransaction) => new TransactionBlock(recentTransaction.commandSet) :: runningTransactions
      case None => new TransactionBlock(Set[UpdateCommand]()) :: runningTransactions
    }
  }

  def commitAll() = {
    // 2*O(n) - because reverse the list is O(n). There is a O(n) solution to this. But kinda lazy now.
    runningTransactions.headOption.foreach { transaction =>
      transaction.commandSet.foreach { cmd =>
        cmd match {
          case SetCommand(key, value) => super.set(key, value)
          case UnSetCommand(key) => super.unSet(key)
        }
      }
    }
    runningTransactions = Nil
  }

  def rollbackRecentTransaction() = {
    if(runningTransactions.isEmpty) {
      println("NO TRANSACTION")
    } else {
      runningTransactions = runningTransactions.tail
    }
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val redis = new MyRedis
    var endSeen = false
    while (!endSeen) {
      val line = readLine()
      val commands = line.split("\\s").map(_.trim)
      commands.head.toUpperCase match {
        case "END" => endSeen = true
        case "SET" => {
          redis.set(commands(1), commands(2))
        }
        case "GET" => {
          println(redis.get(commands(1)))
        }
        case "NUMEQUALTO" => {
          println(redis.numEqualTo(commands(1)))
        }
        case "UNSET" => {
          redis.unSet(commands(1))
        }
        case "BEGIN" => {
          redis.beginTransaction()
        }
        case "COMMIT" => {
          redis.commitAll()
        }
        case "ROLLBACK" => {
          redis.rollbackRecentTransaction()
        }
      }
    }
  }
}
