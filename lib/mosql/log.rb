module MoSQL
  module Logging
    def log
      @@logger ||= Log4r::Logger.new("Stripe::MoSQL")
    end
  end
end
