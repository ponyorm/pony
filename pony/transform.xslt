<?xml version="1.0"?>
<xsl:stylesheet version = '1.0' xmlns:xsl='http://www.w3.org/1999/XSL/Transform'>

  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

  <xsl:template match="header|footer|sidebar" />
  
  <xsl:variable name="styles" select="/html/head/link[@rel='stylesheet' and @type='text/css'] | /html/head/style" />
  <xsl:variable name="layout" select="/html/body/layout" />
  <xsl:variable name="layout-width" select="($layout/@width)[1]"/>
  <xsl:variable name="header" select="/html/body/header" />
  <xsl:variable name="footer" select="/html/body/footer" />
  <xsl:variable name="sidebar" select="/html/body/sidebar" />
  <xsl:variable name="sidebar-width" select="($sidebar/@width)[1]"/>
  <xsl:variable name="content" select="/html/body/content" />
  <xsl:variable name="has_layout" select="$header or $footer or $sidebar or $content or $layout" />

  <xsl:template match="/">
    <html>
      <head>
        <xsl:apply-templates select="@*" />
        <xsl:call-template name="styles" />
        <xsl:apply-templates select="/html/head/*" />
      </head>
      <body>
        <xsl:choose>
          <xsl:when test="$has_layout">
            <xsl:call-template name="layout" />
          </xsl:when>
          <xsl:when test="not($styles)">
            <xsl:call-template name="min-layout" />
          </xsl:when>
          <xsl:otherwise>
            <xsl:apply-templates select="/html/body/node()" />
          </xsl:otherwise>
        </xsl:choose>
      </body>
    </html>
  </xsl:template>

  <xsl:template name="styles">
    <xsl:variable name="pony_static_dir">/pony/static</xsl:variable>
    <xsl:if test="not($styles)">
      <link rel="stylesheet" type="text/css" href="{$pony_static_dir}/yui/reset-fonts-grids/reset-fonts-grids.css" />
      <link rel="stylesheet" type="text/css" href="{$pony_static_dir}/css/pony-default.css" />
    </xsl:if>
    <xsl:if test="$layout-width &gt;=10 and $layout-width != 750 and $layout-width != '800x600' and $layout-width != 950 and $layout-width != '1024x768'">
      <xsl:call-template name="layout-width-style" />
    </xsl:if>
  </xsl:template>

  <xsl:template name="layout-width-style">
    <style>
      <xsl:variable name="em-width" select="$layout-width div 13" />
      <xsl:variable name="em-width-ie" select="$em-width * 0.9759" />
      <xsl:text>#doc-custom { margin:auto; text-align: left; width: </xsl:text>
      <xsl:value-of select="$em-width" />
      <xsl:text>em; *width:</xsl:text>
      <xsl:value-of select="$em-width-ie" />
      <xsl:text>em; min-width: </xsl:text>
      <xsl:value-of select="$layout-width" />
      <xsl:text>px; }</xsl:text>
    </style>
  </xsl:template>
 
  <xsl:template name="min-layout">
    <div id="doc3" class="yui-t7">
      <div id="bd">
        <div id="yui-main">
          <div class="yui-b pony-content">
            <xsl:apply-templates select="/html/body/node()" />
          </div>
        </div>
      </div>
    </div>
  </xsl:template>
  
  <xsl:template name="layout">
    <div>
      <xsl:call-template name="doc-id" />
      <xsl:call-template name="doc-class" />
      <xsl:call-template name="header" />
      <div id="bd">
        <xsl:choose>
          <xsl:when test="$sidebar[@first]">
            <xsl:call-template name="sidebar" />
            <xsl:call-template name="main" />
          </xsl:when>
          <xsl:otherwise>
            <xsl:call-template name="main" />
            <xsl:call-template name="sidebar" />
          </xsl:otherwise>
        </xsl:choose>
      </div>
      <xsl:call-template name="footer" />
    </div>
  </xsl:template>

  <xsl:template name="doc-id">
    <xsl:choose>
      <xsl:when test="$layout-width=750 or $layout-width='800x600'">
        <xsl:attribute name="id">doc</xsl:attribute>
      </xsl:when>
      <xsl:when test="$layout-width=950 or $layout-width='1024x768'">
        <xsl:attribute name="id">doc2</xsl:attribute>
      </xsl:when>
      <xsl:when test="number($layout-width) &gt; 10">
        <xsl:attribute name="id">doc-custom</xsl:attribute>
      </xsl:when>
      <xsl:otherwise>
        <xsl:attribute name="id">doc3</xsl:attribute>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
  
  <xsl:template name="doc-class">
    <xsl:attribute name="class">
      <xsl:choose>
        <xsl:when test="not($sidebar)">yui-t7</xsl:when>
        <xsl:when test="/html/body/sidebar[@right or @align='right']">
          <xsl:choose>
            <xsl:when test="not($sidebar-width) or $sidebar-width=180">yui-t4</xsl:when>
            <xsl:when test="$sidebar-width=240">yui-t5</xsl:when>
            <xsl:when test="$sidebar-width=300">yui-t6</xsl:when>
            <xsl:otherwise>yui-t4</xsl:otherwise>
          </xsl:choose>
        </xsl:when>
        <xsl:otherwise>
          <xsl:choose>
            <xsl:when test="not($sidebar-width) or $sidebar-width=180">yui-t2</xsl:when>
            <xsl:when test="$sidebar-width=160">yui-t1</xsl:when>
            <xsl:when test="$sidebar-width=300">yui-t3</xsl:when>
            <xsl:otherwise>yui-t4</xsl:otherwise>
          </xsl:choose>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:attribute>
  </xsl:template>
  
  <xsl:template name="header">
    <div id="hd" class="pony-header">
      <xsl:apply-templates select="$header/node()" />
    </div>
  </xsl:template>

  <xsl:template name="footer">
    <div id="ft" class="pony-footer">
      <xsl:apply-templates select="$footer/node()" />
    </div>
  </xsl:template>

  <xsl:template name="sidebar">
    <div class="yui-b pony-sidebar">
      <xsl:apply-templates select="$sidebar/node()" />
    </div>
  </xsl:template>

  <xsl:template name="main">
    <div id="yui-main">
      <div class="yui-b pony-content">
        <xsl:apply-templates select="$content" />
      </div>
    </div>
  </xsl:template>

  <xsl:template match="content">
    <xsl:variable name="col_count" select="count(column)" />
    <xsl:choose>
      <xsl:when test="$col_count=1">
        <xsl:apply-templates select="column/node()" />
      </xsl:when>
      <xsl:when test="$col_count=2">
        <div>
          <xsl:choose>
            <xsl:when test="@pattern='2/3-1/3'">
              <xsl:attribute name="class">yui-gc</xsl:attribute>
            </xsl:when>
            <xsl:when test="@pattern='1/3-2/3'">
              <xsl:attribute name="class">yui-gd</xsl:attribute>
            </xsl:when>
            <xsl:when test="@pattern='3/4-1/4'">
              <xsl:attribute name="class">yui-ge</xsl:attribute>
            </xsl:when>
            <xsl:when test="@pattern='1/4-3/4'">
              <xsl:attribute name="class">yui-gf</xsl:attribute>
            </xsl:when>
            <xsl:otherwise>
              <xsl:attribute name="class">yui-g</xsl:attribute>
            </xsl:otherwise>
          </xsl:choose>
          <div class="yui-u first">
            <xsl:apply-templates select="column[1]/node()" />
          </div>
          <div class="yui-u">
            <xsl:apply-templates select="column[2]/node()" />
          </div>
        </div>
      </xsl:when>
      <xsl:when test="$col_count=3">
        <div class="yui-gb">
          <div class="yui-u first">
            <xsl:apply-templates select="column[1]/node()" />
          </div>
          <div class="yui-u">
            <xsl:apply-templates select="column[2]/node()" />
          </div>
          <div class="yui-u">
            <xsl:apply-templates select="column[3]/node()" />
          </div>
        </div>
      </xsl:when>
      <xsl:when test="$col_count=4">
        <div class="yui-g">
          <div class="yui-g first">
            <div class="yui-u first">
              <xsl:apply-templates select="column[1]/node()" />
            </div>
            <div class="yui-u">
              <xsl:apply-templates select="column[2]/node()" />
            </div>
          </div>
          <div class="yui-g">
            <div class="yui-u first">
              <xsl:apply-templates select="column[3]/node()" />
            </div>
            <div class="yui-u">
              <xsl:apply-templates select="column[4]/node()" />
            </div>
          </div>
        </div>
      </xsl:when>
      <xsl:otherwise>
        <p>Wrong column count: <xsl:value-of select="$col_count"/></p>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="content[@pattern='1/4-1/4-1/2']">
    <div class="yui-g">
      <div class="yui-g first">
        <div class="yui-u first">
          <xsl:apply-templates select="column[1]/node()" />
        </div>
        <div class="yui-u">
          <xsl:apply-templates select="column[2]/node()" />
        </div>
      </div>
      <div class="yui-u">
        <xsl:apply-templates select="column[3]/node()" />
      </div>
    </div>
  </xsl:template>

  <xsl:template match="content[@pattern='1/2-1/4-1/4']">
    <div class="yui-g">
      <div class="yui-u first">
        <xsl:apply-templates select="column[1]/node()" />
      </div>
      <div class="yui-g">
        <div class="yui-u first">
          <xsl:apply-templates select="column[2]/node()" />
        </div>
        <div class="yui-u">
          <xsl:apply-templates select="column[3]/node()" />
        </div>
      </div>
    </div>
  </xsl:template>

</xsl:stylesheet>