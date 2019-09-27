using System;
using System.Collections.Generic;
using System.Threading;
using MongoDB.Driver;
using CircuitBreakerSample.MongoDatabase;
using CircuitBreakerSample.Exceptions;

namespace CircuitBreakerSample
{
    public class CircuitBreakerRepository : IPersonRepository
    {
        private CircuitBreakerState _state;
        private MongoPersonRepository _repository;

        public CircuitBreakerRepository(MongoClient client)
        {
            _state = new CircuitBreakerClosed(this);
            _repository = new MongoPersonRepository(client);
        }

        public List<Person> Read()
        {
            return _state.HandleRead();
        }

        public void Write(Person person) 
        {
            _state.HandleWrite(person);
        }
        private abstract class CircuitBreakerState 
        {
            protected CircuitBreakerRepository _owner;
            public CircuitBreakerState(CircuitBreakerRepository owner)
            {
                _owner = owner;
            }
            public abstract List<Person> HandleRead();
            public abstract void HandleWrite(Person person); 
        }

        private class CircuitBreakerClosed : CircuitBreakerState
        {
            private int _errorCount = 0;
            public CircuitBreakerClosed(CircuitBreakerRepository owner)
                :base(owner){}
            
            public override List<Person> HandleRead()
            {
                try
                {
                    return _owner._repository.Read();       
                }
                catch (Exception e) 
                {
                    _trackErrors(e);
                    throw e;
                }
            }

            public override void HandleWrite(Person person)
            {
                try
                {
                    _owner._repository.Write(person);       
                }
                catch (Exception e) 
                {
                    _trackErrors(e);
                    throw e;
                }
            }

            private void _trackErrors(Exception e) 
            {
                _errorCount += 1;
                if (_errorCount > Config.CircuitClosedErrorLimit) 
                {
                    _owner._state = new CircuitBreakerOpen(_owner);
                }
            }
        }
        private class CircuitBreakerOpen : CircuitBreakerState
        {
            
            public CircuitBreakerOpen(CircuitBreakerRepository owner)
                :base(owner)
            {
                new Timer( _ => 
                { 
                    owner._state = new CircuitBreakerHalfOpen(owner); 
                }, null, Config.CircuitOpenTimeout, Timeout.Infinite);
            }

            public override List<Person> HandleRead()
            { 
                throw new CircuitOpenException();
            }
            public override void HandleWrite(Person person)
            {
                throw new CircuitOpenException();
            }
        }
        private class CircuitBreakerHalfOpen : CircuitBreakerState
        {
            private static readonly string Message = "Call failed when circuit half open";
            public CircuitBreakerHalfOpen(CircuitBreakerRepository owner)
                :base(owner){}

            public override List<Person> HandleRead()
            { 
                try 
                {
                    var result = _owner._repository.Read();
                    _owner._state = new CircuitBreakerClosed(_owner);
                    return result;
                }
                catch (Exception e) 
                {
                    _owner._state = new CircuitBreakerOpen(_owner);
                    throw new CircuitOpenException(Message, e);
                }
            }
            public override void HandleWrite(Person person)
            {
                try 
                {
                    _owner._repository.Write(person);
                    _owner._state = new CircuitBreakerClosed(_owner);
                }
                catch (Exception e) 
                {
                    _owner._state = new CircuitBreakerOpen(_owner);
                    throw new CircuitOpenException(Message, e);
                }
            }
        }
    }
}