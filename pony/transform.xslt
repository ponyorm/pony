<?xml version="1.0"?>
<xsl:stylesheet version = '1.0' xmlns:xsl='http://www.w3.org/1999/XSL/Transform'>

  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

  <xsl:template match="header|footer|sidebar" />
  
  <xsl:variable name="styles" select="/html/head/link[@rel='stylesheet' and @type='text/css'] | /html/head/style" />
  <xsl:variable name="header" select="/html/body/header" />
  <xsl:variable name="footer" select="/html/body/footer" />
  <xsl:variable name="sidebar" select="/html/body/sidebar" />
  <xsl:variable name="content" select="/html/body/content" />
  <xsl:variable name="has_layout" select="$header or $footer or $sidebar or $content" />

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
        <div class="yui-g">
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
        <p>Wrong column count!</p>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
  
</xsl:stylesheet>