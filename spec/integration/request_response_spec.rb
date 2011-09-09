require File.expand_path('spec_helper', File.dirname(__FILE__) + '/../')

describe EM::Mongo::RequestResponse do
  before :each do
    @response = EM::Mongo::RequestResponse.new
  end
  context "when first initialized" do
    it "should not be complete" do
      @response.completed?.should be_false
    end
    it "should not have succeeded" do
      @response.succeeded?.should be_false
    end
    it "should not have failed" do
      @response.failed?.should be_false
    end
    it "should not have any data" do
      @response.data.should be_nil
    end
    it "should not have any error" do
      @response.error.should be_nil
    end
  end
  context "when succeeded" do
    before(:each) { @response.succeed [:some,:data] }

    it "should have completed" do
      @response.completed?.should be_true
    end
    it "should have succeeded" do
      @response.succeeded?.should be_true
    end
    it "should not have failed" do
      @response.failed?.should be_false
    end
    it "should have data" do
      @response.data.should == [:some, :data]
    end
    it "should not have an error" do
      @response.error.should be_nil
    end
  end
  context "when failed" do
    before(:each) { @response.fail [RuntimeError, "crap!"]}

    it "should have completed" do
      @response.completed?.should be_true
    end
    it "should not have succeeded" do
      @response.succeeded?.should be_false
    end
    it "should have failed" do
      @response.failed?.should be_true
    end
    it "should not have data" do
      @response.data.should be_nil
    end
    it "should have an error" do
      @response.error.should == [RuntimeError, "crap!"]
    end
  end

end