<?xml version="1.0"?>
<xsl:stylesheet version = '1.0' xmlns:xsl='http://www.w3.org/1999/XSL/Transform'>

  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

  <!--xsl:template match="address|blockquote|center|dir|div|dl|fieldset|form|h1|h2|h3|h4|h5|h6|hr|isindex|menu|noframes|noscript|ol|p|pre|table|ul">
    <xsl:copy>
      <xsl:if test="not(@class)">
        <xsl:attribute name="class">none</xsl:attribute>
      </xsl:if>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template-->

  <xsl:template match="/">
    <xsl:variable name="pony_css_dir">/pony/static/css</xsl:variable>
    <html>
      <head>
        <xsl:apply-templates select="@*" />
        <xsl:choose>
          <xsl:when test="not(/html/head/link[@rel='stylesheet' and @type='text/css']) and not(/html/head/style)">
            <link rel="stylesheet" type="text/css" href="{$pony_css_dir}/grids-min.css" />
            <link rel="stylesheet" type="text/css" href="{$pony_css_dir}/pony-default.css" />
          </xsl:when>
          <xsl:when test="/html/*[header|footer|sidebar]">
            <link rel="stylesheet" type="text/css" href="{$pony_css_dir}/grids-min.css" />
          </xsl:when>
        </xsl:choose>
        <xsl:apply-templates select="/html/head/node()[not(self::header or self::footer or self::sidebar)]" />
      </head>
      <body>
        <xsl:choose>
          <xsl:when test="/html/*[header|footer|sidebar]">
            <xsl:call-template name="layout" />
          </xsl:when>
          <xsl:otherwise>
            <xsl:apply-templates select="/html/body/node()[not(self::header or self::footer or self::sidebar)]" />
          </xsl:otherwise>
        </xsl:choose>
      </body>
    </html>
  </xsl:template>

  <xsl:template name="layout">
    <div id="doc2" class="yui-t2">
      <xsl:call-template name="header" />
      <div id="bd">
        <xsl:choose>
          <xsl:when test="/html/*/sidebar[@first]">
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
      <xsl:apply-templates select="/html/*/header/node()" />
    </div>
  </xsl:template>

  <xsl:template name="footer">
    <div id="ft" class="pony-footer">
      <xsl:apply-templates select="/html/*/footer/node()" />
    </div>
  </xsl:template>

  <xsl:template name="sidebar">
    <div class="yui-b pony-sidebar">
      <xsl:apply-templates select="/html/*/sidebar/node()" />
    </div>
  </xsl:template>

  <xsl:template name="main">
    <div id="yui-main">
      <div class="yui-b pony-content">
        <xsl:apply-templates select="/html/body/*[not(self::header or self::footer or self::sidebar)]" />
      </div>
    </div>
  </xsl:template>

</xsl:stylesheet>