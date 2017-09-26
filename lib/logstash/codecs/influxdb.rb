# encoding: utf-8
require "logstash/codecs/base"
require "logstash/util/charset"
#require "logstash/timestamp"

# InfluxDB line protocol.
# Decoding behavior: Only whole line events will be emitted.
# Encoding behavior: Not implemented.
class LogStash::Codecs::InfluxDB < LogStash::Codecs::Base
  config_name "influxdb"

  # Set the desired text format for encoding.
  config :format, :validate => :string

  public
  def register
    require 'bigdecimal'
    def prepare_type(in_object)
      return in_object.to_s.gsub!(/\A"|"\Z/, '') unless (in_object =~ /[\d+\.]/)
      num = BigDecimal.new(in_object.to_s)
      if num.frac == 0
        return num.to_i
      else
        return num.to_f
      end
    end
  end

  public
  def decode(payload)
    lines_array = payload.split("\n")
    lines_array.each do |line|
      #head, data, timestamp = line.split(" ")
      #It splits if there is a space but only if the text following until the end contains an even number of "
      head, data, timestamp = line.split(/\s(?=(?:[^"]|"[^"]*")*$)/) 
      measurement_name, tags_string = head.split(",", 2)
      #fields_array = data.split(",")
      fields_array = data.split(/,(?=(?:[^"]|"[^"]*")*$)/)
      tags_array = tags_string.split(",")
      tags = {}
      tags_array.each do |tag|
        tag_key, tag_value = tag.split("=")
        tags[tag_key] = tag_value
      end
      decoded = {}
      decoded["@timestamp"] = LogStash::Timestamp.at(timestamp.to_i/1000000000) # nanoseconds utc
      decoded["measurement"] = measurement_name
      decoded["tag"] = tags
      fields_array.each do |field|
        field_key, field_value = field.split("=")
        if field_key == "type" then
          field_key = "preserved_type"
        end
        decoded[field_key] = prepare_type(field_value)
      end
      yield LogStash::Event.new(decoded)
    end
  end # def decode

  public
  def encode(event)
    raise "Not implemented"
  end # def encode

end # class LogStash::Codecs::Plain


