<?xml version="1.0"?>
<xsl:stylesheet version = '1.0' xmlns:xsl='http://www.w3.org/1999/XSL/Transform'>

  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

  <xsl:template match="header|footer|sidebar" />
  
  <!--xsl:template match="address|blockquote|center|dir|div|dl|fieldset|form|h1|h2|h3|h4|h5|h6|hr|isindex|menu|noframes|noscript|ol|p|pre|table|ul">
    <xsl:copy>
      <xsl:if test="not(@class)">
        <xsl:attribute name="class">none</xsl:attribute>
      </xsl:if>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template-->

  <xsl:variable name="styles" select="/html/head/link[@rel='stylesheet' and @type='text/css'] | /html/head/style" />
  <xsl:variable name="header" select="//header" />
  <xsl:variable name="footer" select="//footer" />
  <xsl:variable name="sidebar" select="//sidebar" />

  <xsl:template match="/">
    <xsl:variable name="pony_static_dir">/pony/static</xsl:variable>
    <html>
      <head>
        <xsl:apply-templates select="@*" />
        <xsl:if test="not($styles)">
          <link rel="stylesheet" type="text/css" href="{$pony_static_dir}/yui/reset-fonts-grids/reset-fonts-grids.css" />
          <link rel="stylesheet" type="text/css" href="{$pony_static_dir}/css/pony-default.css" />
          <!--xsl:choose>
            <xsl:when test="$header or $footer or $sidebar">
              <link rel="stylesheet" type="text/css" href="{$pony_static_dir}/yui/reset-fonts-grids/reset-fonts-grids.css" />
              <link rel="stylesheet" type="text/css" href="{$pony_static_dir}/css/pony-default.css" />
            </xsl:when>
            <xsl:otherwise>
              <link rel="stylesheet" type="text/css" href="{$pony_static_dir}/yui/reset/reset-min.css" />
              <link rel="stylesheet" type="text/css" href="{$pony_static_dir}/yui/fonts/fonts-min.css" />
              <link rel="stylesheet" type="text/css" href="{$pony_static_dir}/css/pony-default.css" />
            </xsl:otherwise>
          </xsl:choose-->
        </xsl:if>
        <xsl:apply-templates select="/html/head/*" />
      </head>
      <body>
        <xsl:choose>
          <xsl:when test="$header or $footer or $sidebar">
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

  <xsl:template name="min-layout">
    <div id="doc3" class="yui-t7">
      <div id="bd">
        <xsl:call-template name="main" />
      </div>
    </div>
  </xsl:template>
  
  <xsl:template name="layout">
    <div id="doc2" class="yui-t2">
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
        <xsl:apply-templates select="/html/body/node()" />
      </div>
    </div>
  </xsl:template>

</xsl:stylesheet>