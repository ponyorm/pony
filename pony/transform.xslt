<?xml version="1.0"?>
<xsl:stylesheet version = '1.0' xmlns:xsl='http://www.w3.org/1999/XSL/Transform'>
  
  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

  <xsl:variable name="styles" select="/html/head/link[@blueprint or (@rel='stylesheet' and @type='text/css')] | /html/head/style" />

  <xsl:template match="head">
    <head>
      <xsl:apply-templates select="@*" />
      <xsl:if test="not(boolean($styles))">
        <xsl:call-template name="blueprint-std" />
      </xsl:if>
      <xsl:apply-templates select="*" />
    </head>
  </xsl:template>

  <xsl:template name="blueprint-std" match="link[@blueprint]">
    <link rel="stylesheet" href="/pony/static/blueprint/screen.css" type="text/css" media="screen, projection" />
    <link rel="stylesheet" href="/pony/static/blueprint/print.css" type="text/css" media="print" />
    <xsl:comment>{{[if IE]}}</xsl:comment>
    <link rel="stylesheet" href="/pony/static/blueprint/ie.css" type="text/css" media="screen, projection" />
    <xsl:comment>{{[endif]}}</xsl:comment>
  </xsl:template>

  <xsl:template match="link[@rounded-corners]">
    <link href="/pony/static/css/rounded-corners.css" type="text/css" rel="stylesheet" />
  </xsl:template>
  
  <xsl:template match="body">
    <body>
      <xsl:choose>
        <xsl:when test="$styles or div[contains(concat(' ', @class, ' '), ' container ')]">
          <xsl:apply-templates select="node()" />
        </xsl:when>
        <xsl:otherwise>
          <div class="container">
            <xsl:apply-templates select="node()" />
          </div>
        </xsl:otherwise>
      </xsl:choose>
    </body>
  </xsl:template>

  <xsl:template match="*[@radius]">
    <xsl:copy>
      <xsl:apply-templates select="@*[name()!='radius']" />
      <xsl:attribute name="class">
        <xsl:value-of select="normalize-space(concat(@class, ' ', 'rounded'))"/>
      </xsl:attribute>
      <xsl:apply-templates select="node()" />
      <div class="top-left radius-{@radius}"></div>
      <div class="top-right radius-{@radius}"></div>
      <div class="bottom-left radius-{@radius}"></div>
      <div class="bottom-right radius-{@radius}"></div>      
    </xsl:copy>
  </xsl:template>

  <xsl:template match="a[starts-with(@href, 'mailto:') and not(@onmousedown) and not(@onkeydown) and not(@no-obfuscated)]">
    <xsl:copy>
      <xsl:apply-templates select="@*" />
      <xsl:attribute name="href">
        <xsl:value-of select="concat(substring-before(@href, '@'), 'ATSIGN', substring-after(@href, '@'))" />
      </xsl:attribute>
      <xsl:attribute name="onmousedown">this.href=this.href.replace('ATSIGN','@')</xsl:attribute>
      <xsl:attribute name="onkeydown">this.onmousedown()</xsl:attribute>
      <xsl:apply-templates select="node()" />
    </xsl:copy>
  </xsl:template>

  <xsl:template match="@no-obfuscated"></xsl:template>

</xsl:stylesheet>
