# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "rapids/version"



Gem::Specification.new do |s|
  rapids_summary = "A ruby library to provide rapid insertion of rows into a database"
  
  s.name        = "rapids"
  s.version     = Rapids::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ['James Smith']
  s.email       = ["TODO: Write your email address"]
  s.homepage    = 'http://github.com/sunblaze/rapids'
  s.summary     = rapids_summary
  s.description = rapids_summary #TODO write a better long form description
  s.license     = 'MIT'
  s.required_ruby_version = '>= 1.8.7'

  s.rubyforge_project = "rapids"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
  
  s.add_development_dependency "rspec", "2.5.0"
  s.add_development_dependency "activerecord", "~> 3.0.6"
  s.add_development_dependency "mysql", "~> 2.8.1"
end
