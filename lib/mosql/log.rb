module MoSQL
  module Logging
    def log
      return @@logger if defined? @@logger

      @@logger ||= Log4r::Logger.new("Stripe::MoSQL")
      outputter = Log4r::StdoutOutputter.new(STDERR)
      outputter.formatter = Log4r::PatternFormatter.new(
        :pattern => "- %d [%l]: %M",
        :date_pattern => "%Y-%m-%d %H:%M:%S.%L")

      @@logger.outputters = outputter
      @@logger
    end
  end
end
