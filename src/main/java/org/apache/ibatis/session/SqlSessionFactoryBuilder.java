/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.session;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;

import org.apache.ibatis.builder.xml.XMLConfigBuilder;
import org.apache.ibatis.exceptions.ExceptionFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.session.defaults.DefaultSqlSessionFactory;

/**
 *
 * 构建SqlSessionFactory的构建者，构建者模式。
 *
 * mybatis文档对于此类的作用域的描述
 *
 * <p>
 * 这个类可以被实例化、使用和丢弃，一旦创建了 SqlSessionFactory，就不再需要它了。
 * 因此 SqlSessionFactoryBuilder 实例的最佳作用域是方法作用域（也就是局部方法变量）。
 * 你可以重用 SqlSessionFactoryBuilder 来创建多个 SqlSessionFactory 实例，
 * 但最好还是不要一直保留着它，以保证所有的 XML 解析资源可以被释放给更重要的事情。
 * </p>
 *
 *
 * Builds {@link SqlSession} instances.
 *
 * @author Clinton Begin
 */
public class SqlSessionFactoryBuilder {

  public SqlSessionFactory build(Reader reader) {
    return build(reader, null, null);
  }
  public SqlSessionFactory build(Reader reader, String environment) {
    return build(reader, environment, null);
  }
  public SqlSessionFactory build(Reader reader, Properties properties) {
    return build(reader, null, properties);
  }

  //上面三个方法最终都会调用到本方法

  /**
   * 它使用了一个参照了XML文档或更特定的SqlMapConfig.xml文件的Reader实例。
   * 可选的参数是environment和properties。Environment决定加载哪种环境(开发环境/生产环境)，包括数据源和事务管理器。
   * 如果使用properties，那么就会加载那些properties（属性配置文件），那些属性可以用${propName}语法形式多次用在配置文件中。
   */
  public SqlSessionFactory build(Reader reader, String environment, Properties properties) {
    try {
      //委托给XMLConfigBuilder进行解析
      XMLConfigBuilder parser = new XMLConfigBuilder(reader, environment, properties);
      //开始使用XMLConfigBuilder解析xml中的配置信息，返回一个Configuration，使用Configuration构建一个SqlSessionFactory对象
      return build(parser.parse());
    } catch (Exception e) {
      //包装异常类
      throw ExceptionFactory.wrapException("Error building SqlSession.", e);
    } finally {
      ErrorContext.instance().reset();
      try {
        reader.close();
      } catch (IOException e) {
        // Intentionally ignore. Prefer previous error.
      }
    }
  }

  public SqlSessionFactory build(InputStream inputStream) {
    return build(inputStream, null, null);
  }

  public SqlSessionFactory build(InputStream inputStream, String environment) {
    return build(inputStream, environment, null);
  }

  public SqlSessionFactory build(InputStream inputStream, Properties properties) {
    return build(inputStream, null, properties);
  }

  /**
   *上述三个方法都会调用到本方法
   * 与{@link #build(Reader, String, Properties)} 相似只不过将{@code Reader} 换成了{@code InputStream}
   */
  public SqlSessionFactory build(InputStream inputStream, String environment, Properties properties) {
    try {
      XMLConfigBuilder parser = new XMLConfigBuilder(inputStream, environment, properties);
      return build(parser.parse());
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error building SqlSession.", e);
    } finally {
      ErrorContext.instance().reset();
      try {
        inputStream.close();
      } catch (IOException e) {
        // Intentionally ignore. Prefer previous error.
      }
    }
  }


  /**
   * 上面的两个方法通过流中的配置获取到Configuration对象后都会调用本方法生成SqlSessionFactory对象
   */
  public SqlSessionFactory build(Configuration config) {
    return new DefaultSqlSessionFactory(config);
  }

}
