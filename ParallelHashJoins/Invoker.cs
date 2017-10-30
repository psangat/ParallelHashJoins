using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    public static class Invoker
    {
        public static object CreateAndInvoke(string typeName, object[] constructorArgs, string methodName, object[] methodArgs)
        {
            Type type = Type.GetType(typeName);
            object instance = Activator.CreateInstance(type, constructorArgs);

            MethodInfo method = type.GetMethod(methodName);
            return method.Invoke(instance, methodArgs);
        }
    }
}
