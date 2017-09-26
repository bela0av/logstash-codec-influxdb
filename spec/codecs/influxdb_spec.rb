# encoding: utf-8

require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/line"
require "logstash/event"

describe LogStash::Codecs::Line do
  subject do
    next LogStash::Codecs::Line.new
  end

  context "#encode" do
    it "should raise an exception" do
      expect { subject.decode }.to raise_error
    end
  end

  context "#decode" do
    it "should return an event from influxdb line" do
      decoded = false
      subject.decode("hello world\n") do |e|
        decoded = true
        insist { e.is_a?(LogStash::Event) }
        insist { e.get("message") } == "hello world"
      end
      insist { decoded } == true
    end

  end

end
