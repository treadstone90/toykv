/**
 * Created by karthik on 10/11/15.
 */

class TransactionBlock(initialCommandMap: Set[UpdateCommand]) {
  private var transactionLog = initialCommandMap.map(command => command.key -> command).toMap
  def addCommand(command: UpdateCommand): Unit = {
    transactionLog = transactionLog.updated(command.key, command)
  }

  def getCommand(key: String): Option[UpdateCommand] = {
    transactionLog.get(key)
  }

  def contains(key: String) = transactionLog.contains(key)

  def commandSet = transactionLog.values.toSet
}

