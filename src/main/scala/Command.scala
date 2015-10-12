/**
 * Created by karthik on 10/6/15.
 * A transaction block is a list of update commands
 */
trait Command

trait UpdateCommand extends Command {
  def key: String
}

case class SetCommand(key: String, value: String) extends UpdateCommand
case class GetCommand(variable: String) extends Command
case class UnSetCommand(key: String) extends UpdateCommand
case class NumEqualTo(value: String) extends Command
case object End extends Command

