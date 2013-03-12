module MoSQL

  class Callback
    include MoSQL::Logging

    def initialize(db)
      @db = db
    end

    def after_upsert(obj)
    end

    def after_delete(obj)
    end

  end
end
